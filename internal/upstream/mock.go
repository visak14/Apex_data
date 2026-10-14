package upstream

import (
	"math/rand"
	"sync"
	"time"

	"market-data-engine/internal/models"
)

// Base prices for 61 symbols for high-fidelity realistic simulation
var initialPrices = map[string]float64{
	"AUDUSD": 0.6520, "AUDCAD": 0.8950, "AUDCHF": 0.5810, "AUDJPY": 98.40, "AUDNZD": 1.0850,
	"AUDSGD": 0.8650, "CADCHF": 0.6480, "CADJPY": 109.80, "CHFJPY": 169.20, "CHFSGD": 1.4880,
	"EURAUD": 1.6350, "EURCAD": 1.4640, "EURCHF": 0.9505, "EURGBP": 0.8350, "EURJPY": 160.80,
	"EURNZD": 1.7740, "EURUSD": 1.0660, "GBPAUD": 1.9580, "GBPCAD": 1.7530, "GBPCHF": 1.1380,
	"GBPJPY": 192.50, "GBPNZD": 2.1240, "GBPUSD": 1.2760, "NZDCAD": 0.8250, "NZDCHF": 0.5350,
	"NZDJPY": 90.60, "NZDUSD": 0.6010, "USDCAD": 1.3730, "USDCHF": 0.8910, "USDCNH": 7.2450,
	"USDJPY": 150.90, "USDMXN": 19.85, "USDNOK": 10.95, "USDPLN": 4.1200, "USDSEK": 10.82,
	"USDSGD": 1.3260, "USDTRY": 34.25, "USDZAR": 18.15, "EURTRY": 36.50, "GBPTRY": 43.70,
	"NOKJPY": 13.78, "SEKJPY": 13.94, "SGDJPY": 113.80, "ZARJPY": 8.31, "XAUUSD": 2680.50,
	"XAGUSD": 31.45, "BTCUSD": 68450.0, "ETHUSD": 2620.0, "BNBUSD": 585.0, "SOLUSD": 158.0,
	"XRPUSD": 0.5420, "ADAUSD": 0.3550, "DOGUSD": 0.1250, "DOTUSD": 4.25, "LTCUSD": 68.50,
	"BCHUSD": 345.0, "XLMUSD": 0.0980, "TRXUSD": 0.1620, "UNIUSD": 7.85, "FILUSD": 3.75,
	"AVXUSD": 28.50,
}

type MockFeed struct {
	symbols    []string
	prices     map[string]float64
	ingestChan chan<- models.RawMarketTick
	stopChan   chan struct{}
	mu         sync.Mutex
	running    bool
}

func NewMockFeed(symbols []string, ingestChan chan<- models.RawMarketTick) *MockFeed {
	prices := make(map[string]float64)
	for _, s := range symbols {
		if p, ok := initialPrices[s]; ok {
			prices[s] = p
		} else {
			prices[s] = 100.0
		}
	}

	return &MockFeed{
		symbols:    symbols,
		prices:     prices,
		ingestChan: ingestChan,
		stopChan:   make(chan struct{}),
	}
}

func (mf *MockFeed) Start() {
	mf.mu.Lock()
	if mf.running {
		mf.mu.Unlock()
		return
	}
	mf.running = true
	mf.mu.Unlock()

	go func() {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		// Ticker fires every 20-50ms to generate realistic high frequency ticks
		ticker := time.NewTicker(25 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-mf.stopChan:
				return
			case <-ticker.C:
				// Pick 1 to 5 random symbols to update in each tick
				numUpdates := r.Intn(4) + 1
				for i := 0; i < numUpdates; i++ {
					sym := mf.symbols[r.Intn(len(mf.symbols))]
					currPrice := mf.prices[sym]

					// Random walk price change between -0.15% and +0.15%
					deltaPct := (r.Float64() - 0.499) * 0.003
					newPrice := currPrice * (1.0 + deltaPct)
					mf.prices[sym] = newPrice

					// Spread between buy and sell
					spread := newPrice * 0.0001
					now := time.Now().UnixMilli()

					tick := models.RawMarketTick{
						Symbol:    sym,
						Buy:       newPrice - (spread / 2),
						Sell:      newPrice + (spread / 2),
						Timestamp: now,
						// Deliberately provide dummy values to prove they are ignored
						Low24h:    0.0001,
						High24h:   9999999,
						Change24h: -99.9,
					}

					select {
					case mf.ingestChan <- tick:
					default:
					}
				}
			}
		}
	}()
}

func (mf *MockFeed) Stop() {
	mf.mu.Lock()
	defer mf.mu.Unlock()
	if mf.running {
		close(mf.stopChan)
		mf.running = false
	}
}
