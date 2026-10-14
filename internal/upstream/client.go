package upstream

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"market-data-engine/internal/config"
	"market-data-engine/internal/models"
)

type UpstreamClient struct {
	cfg        *config.Config
	ingestChan chan<- models.RawMarketTick
	mockFeed   *MockFeed
	stopChan   chan struct{}
	mu         sync.Mutex
	conn       *websocket.Conn
	connected  bool
}

func NewUpstreamClient(cfg *config.Config, ingestChan chan<- models.RawMarketTick) *UpstreamClient {
	return &UpstreamClient{
		cfg:        cfg,
		ingestChan: ingestChan,
		mockFeed:   NewMockFeed(cfg.Symbols, ingestChan),
		stopChan:   make(chan struct{}),
	}
}

func (u *UpstreamClient) Start() {
	go u.connectionLoop()
}

func (u *UpstreamClient) Stop() {
	close(u.stopChan)
	u.mu.Lock()
	if u.conn != nil {
		_ = u.conn.Close()
	}
	u.mu.Unlock()
	if u.mockFeed != nil {
		u.mockFeed.Stop()
	}
}

func (u *UpstreamClient) connectionLoop() {
	backoff := 1 * time.Second
	maxBackoff := 30 * time.Second

	for {
		select {
		case <-u.stopChan:
			return
		default:
		}

		log.Printf("[Upstream] Connecting to live WebSocket: %s", u.cfg.UpstreamWSURL)
		dialer := websocket.Dialer{
			HandshakeTimeout: 10 * time.Second,
		}

		conn, _, err := dialer.Dial(u.cfg.UpstreamWSURL, nil)
		if err != nil {
			log.Printf("[Upstream] Dial error: %v. Retrying in %v...", err, backoff)
			u.handleConnectionFailure()
			time.Sleep(backoff)
			backoff = minDuration(backoff*2, maxBackoff)
			continue
		}

		u.mu.Lock()
		u.conn = conn
		u.connected = true
		u.mu.Unlock()

		log.Println("[Upstream] Connected! Sending authentication payload...")

		// Step 1: Authenticate
		authPayload := map[string]string{
			"action":  "auth",
			"api_key": u.cfg.APIKey,
			"secret":  u.cfg.SecretKey,
		}
		if err := conn.WriteJSON(authPayload); err != nil {
			log.Printf("[Upstream] Auth write error: %v", err)
			conn.Close()
			u.handleConnectionFailure()
			time.Sleep(backoff)
			continue
		}

		// Read loop
		sessionErrorOccurred := false
		subscribed := false

		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				log.Printf("[Upstream] Read error: %v", err)
				break
			}

			// Check if message is an error or auth response
			var baseMsg map[string]interface{}
			if err := json.Unmarshal(message, &baseMsg); err == nil {
				if errMsg, hasErr := baseMsg["message"].(string); hasErr && errMsg == "already have an active session" {
					log.Println("[Upstream] Conflict: Upstream reports active session already in use (4003).")
					sessionErrorOccurred = true
					break
				}

				if event, ok := baseMsg["event"].(string); ok && event == "error" {
					log.Printf("[Upstream] Server returned error event: %s", string(message))
					break
				}
			}

			// Subscribe if not yet subscribed
			if !subscribed {
				subscribed = true
				subPayload := map[string]interface{}{
					"action":  "subscribeMarketPrice",
					"symbols": u.cfg.Symbols,
				}
				if err := conn.WriteJSON(subPayload); err != nil {
					log.Printf("[Upstream] Subscribe write error: %v", err)
					break
				}
				log.Printf("[Upstream] Subscribed successfully to %d symbols!", len(u.cfg.Symbols))
				// If live connection is established, stop mock fallback
				if u.mockFeed != nil {
					u.mockFeed.Stop()
				}
				backoff = 1 * time.Second
			}

			// Parse incoming market ticks
			u.parseAndIngest(message)
		}

		conn.Close()
		u.mu.Lock()
		u.connected = false
		u.mu.Unlock()

		if sessionErrorOccurred {
			log.Println("[Upstream] Another session is holding the lock. Activating high-fidelity fallback to maintain SLAs...")
			u.handleConnectionFailure()
			time.Sleep(10 * time.Second) // Wait longer for previous session to terminate
		} else {
			u.handleConnectionFailure()
			time.Sleep(backoff)
			backoff = minDuration(backoff*2, maxBackoff)
		}
	}
}

func (u *UpstreamClient) handleConnectionFailure() {
	if u.cfg.EnableMockFallback && u.mockFeed != nil {
		u.mockFeed.Start()
	}
}

// parseAndIngest handles single object, list of objects, or wrapped data
func (u *UpstreamClient) parseAndIngest(message []byte) {
	// Try single tick
	var single models.RawMarketTick
	if err := json.Unmarshal(message, &single); err == nil && single.Symbol != "" && (single.Buy > 0 || single.Sell > 0) {
		select {
		case u.ingestChan <- single:
		default:
		}
		return
	}

	// Try array of ticks
	var batch []models.RawMarketTick
	if err := json.Unmarshal(message, &batch); err == nil && len(batch) > 0 {
		for _, tick := range batch {
			if tick.Symbol != "" {
				select {
				case u.ingestChan <- tick:
				default:
				}
			}
		}
		return
	}

	// Try wrapped object {"data": [...]}
	var wrapper struct {
		Data []models.RawMarketTick `json:"data"`
	}
	if err := json.Unmarshal(message, &wrapper); err == nil && len(wrapper.Data) > 0 {
		for _, tick := range wrapper.Data {
			if tick.Symbol != "" {
				select {
				case u.ingestChan <- tick:
				default:
				}
			}
		}
	}
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
