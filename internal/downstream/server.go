package downstream

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"market-data-engine/internal/models"
	"market-data-engine/internal/processor"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all CORS for frontend development
	},
	EnableCompression: true,
}

type Client struct {
	server       *Server
	conn         *websocket.Conn
	send         chan []byte
	subscribed   map[string]bool // symbol -> true, or "*" -> true
	format       string          // "compact" or "full"
	mu           sync.RWMutex
}

type Server struct {
	port          string
	engine        *processor.Engine
	clients       map[*Client]bool
	register      chan *Client
	unregister    chan *Client
	broadcast     chan models.ProcessedMarketTick
	deltaBuffer   map[string]models.ProcessedMarketTick
	deltaMu       sync.Mutex
	mu            sync.RWMutex
	stopChan      chan struct{}
	bcastInterval time.Duration
}

func NewServer(port string, engine *processor.Engine, bcastIntervalMs int) *Server {
	return &Server{
		port:          port,
		engine:        engine,
		clients:       make(map[*Client]bool),
		register:      make(chan *Client),
		unregister:    make(chan *Client),
		broadcast:     make(chan models.ProcessedMarketTick, 5000),
		deltaBuffer:   make(map[string]models.ProcessedMarketTick),
		stopChan:      make(chan struct{}),
		bcastInterval: time.Duration(bcastIntervalMs) * time.Millisecond,
	}
}

func (s *Server) Start() {
	go s.hubLoop()
	go s.deltaFlusherLoop()

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", s.handleWebSocket)
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/api/symbols", s.handleSymbols)

	server := &http.Server{
		Addr:    ":" + s.port,
		Handler: mux,
	}

	go func() {
		log.Printf("[Server] Custom WebSocket Server listening on http://0.0.0.0:%s/ws", s.port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[Server] HTTP server failed: %v", err)
		}
	}()
}

func (s *Server) BroadcastTick(tick models.ProcessedMarketTick) {
	s.deltaMu.Lock()
	s.deltaBuffer[tick.Symbol] = tick
	s.deltaMu.Unlock()
}

func (s *Server) hubLoop() {
	for {
		select {
		case <-s.stopChan:
			return
		case client := <-s.register:
			s.mu.Lock()
			s.clients[client] = true
			s.mu.Unlock()
			log.Printf("[Server] Client connected. Total active clients: %d", len(s.clients))

			// Send immediate initial snapshot of all symbols upon connection
			s.sendInitialSnapshot(client)

		case client := <-s.unregister:
			s.mu.Lock()
			if _, ok := s.clients[client]; ok {
				delete(s.clients, client)
				close(client.send)
			}
			s.mu.Unlock()
			log.Printf("[Server] Client disconnected. Total active clients: %d", len(s.clients))
		}
	}
}

// deltaFlusherLoop bundles updates into high-efficiency delta packets (<10KB) every bcastInterval
func (s *Server) deltaFlusherLoop() {
	ticker := time.NewTicker(s.bcastInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopChan:
			return
		case <-ticker.C:
			s.deltaMu.Lock()
			if len(s.deltaBuffer) == 0 {
				s.deltaMu.Unlock()
				continue
			}

			// Extract deltas
			updates := make([]models.ProcessedMarketTick, 0, len(s.deltaBuffer))
			compactUpdates := make([]models.CompactMarketTick, 0, len(s.deltaBuffer))
			for _, tick := range s.deltaBuffer {
				updates = append(updates, tick)
				compactUpdates = append(compactUpdates, tick.ToCompact())
			}
			// Reset buffer
			s.deltaBuffer = make(map[string]models.ProcessedMarketTick)
			s.deltaMu.Unlock()

			s.mu.RLock()
			for client := range s.clients {
				s.sendDeltaToClient(client, updates, compactUpdates)
			}
			s.mu.RUnlock()
		}
	}
}

func (s *Server) sendInitialSnapshot(client *Client) {
	snapshots := s.engine.GetAllSnapshots()
	if len(snapshots) == 0 {
		return
	}

	compactList := make([]models.CompactMarketTick, 0, len(snapshots))
	for _, snap := range snapshots {
		compactList = append(compactList, snap.ToCompact())
	}

	msg := models.ServerBroadcastMessage{
		Type:        "snapshot",
		Count:       len(compactList),
		CompactData: compactList,
		ServerTime:  time.Now().UnixMilli(),
	}

	payloadBytes, _ := json.Marshal(msg)
	msg.PayloadKB = float64(len(payloadBytes)) / 1024.0
	// Re-marshal with accurate payload size
	finalBytes, _ := json.Marshal(msg)

	select {
	case client.send <- finalBytes:
	default:
	}
}

func (s *Server) sendDeltaToClient(client *Client, updates []models.ProcessedMarketTick, compact []models.CompactMarketTick) {
	client.mu.RLock()
	defer client.mu.RUnlock()

	// Filter by client subscriptions
	var filteredCompact []models.CompactMarketTick
	var filteredFull []models.ProcessedMarketTick

	isSubAll := client.subscribed["*"] || len(client.subscribed) == 0

	for i, tick := range updates {
		if isSubAll || client.subscribed[tick.Symbol] {
			if client.format == "full" {
				filteredFull = append(filteredFull, tick)
			} else {
				filteredCompact = append(filteredCompact, compact[i])
			}
		}
	}

	if len(filteredCompact) == 0 && len(filteredFull) == 0 {
		return
	}

	msg := models.ServerBroadcastMessage{
		Type:       "delta",
		ServerTime: time.Now().UnixMilli(),
	}

	if client.format == "full" {
		msg.Count = len(filteredFull)
		msg.Data = filteredFull
	} else {
		msg.Count = len(filteredCompact)
		msg.CompactData = filteredCompact
	}

	payloadBytes, _ := json.Marshal(msg)
	msg.PayloadKB = float64(len(payloadBytes)) / 1024.0
	finalBytes, _ := json.Marshal(msg)

	select {
	case client.send <- finalBytes:
	default:
	}
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[Server] Upgrade error: %v", err)
		return
	}

	client := &Client{
		server:     s,
		conn:       conn,
		send:       make(chan []byte, 512),
		subscribed: make(map[string]bool),
		format:     "compact", // Default to compact for <10KB requirement
	}
	// By default subscribe to all
	client.subscribed["*"] = true

	s.register <- client

	go client.writePump()
	go client.readPump()
}

func (c *Client) readPump() {
	defer func() {
		c.server.unregister <- c
		_ = c.conn.Close()
	}()

	c.conn.SetReadLimit(512 * 1024)
	_ = c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.conn.SetPongHandler(func(string) error {
		_ = c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			break
		}

		var clientMsg models.ClientMessage
		if err := json.Unmarshal(message, &clientMsg); err == nil {
			c.mu.Lock()
			if clientMsg.Format != "" {
				c.format = clientMsg.Format
			}
			if clientMsg.Action == "subscribe" {
				c.subscribed = make(map[string]bool)
				for _, sym := range clientMsg.Symbols {
					c.subscribed[sym] = true
				}
			} else if clientMsg.Action == "unsubscribe" {
				for _, sym := range clientMsg.Symbols {
					delete(c.subscribed, sym)
				}
			}
			c.mu.Unlock()
		}
	}
}

func (c *Client) writePump() {
	ticker := time.NewTicker(30 * time.Second)
	defer func() {
		ticker.Stop()
		_ = c.conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.send:
			_ = c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				_ = c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			w, err := c.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			_, _ = w.Write(message)

			// Drain queued messages into the same WebSocket frame for extra throughput
			n := len(c.send)
			for i := 0; i < n; i++ {
				_, _ = w.Write([]byte("\n"))
				_, _ = w.Write(<-c.send)
			}

			if err := w.Close(); err != nil {
				return
			}

		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	clientCount := len(s.clients)
	s.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status":         "healthy",
		"active_clients": clientCount,
		"symbols_loaded": len(s.engine.GetAllSnapshots()),
		"timestamp":      time.Now().UnixMilli(),
	})
}

func (s *Server) handleSymbols(w http.ResponseWriter, r *http.Request) {
	snapshots := s.engine.GetAllSnapshots()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(snapshots)
}
