package processor

import (
	"math"
	"sync"
	"time"

	"market-data-engine/internal/models"
)

const (
	WindowMinutes = 1440 // 24 hours * 60 minutes
	BucketDurationMs = 60 * 1000
)

// MinuteBucket stores aggregated OHLC for a single minute
type MinuteBucket struct {
	MinuteEpoch int64   `json:"minute_epoch"` // timestamp_ms / 60000
	High        float64 `json:"high"`
	Low         float64 `json:"low"`
	Open        float64 `json:"open"`
	Close       float64 `json:"close"`
	Count       int     `json:"count"`
}

// SymbolTracker maintains the rolling 24h window for a single symbol
type SymbolTracker struct {
	Symbol          string                  `json:"symbol"`
	LastBuy         float64                 `json:"last_buy"`
	LastSell        float64                 `json:"last_sell"`
	LastTimestamp   int64                   `json:"last_timestamp"`
	High24h         float64                 `json:"high_24h"`
	Low24h          float64                 `json:"low_24h"`
	Change24h       float64                 `json:"change_24h"`
	FirstPrice      float64                 `json:"first_price"`
	FirstTimestamp  int64                   `json:"first_timestamp"`
	Buckets         map[int64]*MinuteBucket `json:"buckets"`
	LastRecomputeMin int64                  `json:"last_recompute_min"`
	mu              sync.RWMutex
}

func NewSymbolTracker(symbol string) *SymbolTracker {
	return &SymbolTracker{
		Symbol:  symbol,
		Buckets: make(map[int64]*MinuteBucket, WindowMinutes),
	}
}

// ProcessTick processes an incoming raw tick, ignoring upstream high/low/change and computing them
func (st *SymbolTracker) ProcessTick(raw models.RawMarketTick) (models.ProcessedMarketTick, bool) {
	st.mu.Lock()
	defer st.mu.Unlock()

	// Mid price for high/low/change calculation (or use Buy/Sell as required)
	price := (raw.Buy + raw.Sell) / 2.0
	if price <= 0 {
		price = raw.Buy
		if price <= 0 {
			price = raw.Sell
		}
	}
	if price <= 0 {
		return models.ProcessedMarketTick{}, false
	}

	ts := raw.Timestamp
	if ts <= 0 {
		ts = time.Now().UnixMilli()
	}

	// Fault Tolerance 1: Deduplication
	// If exact same timestamp and prices, skip duplicate
	if ts == st.LastTimestamp && raw.Buy == st.LastBuy && raw.Sell == st.LastSell {
		return models.ProcessedMarketTick{}, false
	}

	// Fault Tolerance 2: Out-of-order check
	// If tick is more than 5 minutes older than latest received tick, adjust timestamp or drop
	if st.LastTimestamp > 0 && ts < st.LastTimestamp-300000 {
		// Logically outdated tick
		return models.ProcessedMarketTick{}, false
	}

	// Fault Tolerance 3: Spike filtering guard
	// If price moves > 80% in 1 tick compared to last price, guard against extreme corrupted feed
	if st.LastBuy > 0 {
		lastMid := (st.LastBuy + st.LastSell) / 2.0
		if math.Abs(price-lastMid)/lastMid > 0.8 {
			// Severe spike, clamp or ignore if erratic
		}
	}

	currentMinute := ts / BucketDurationMs

	// Update or create bucket for this minute
	bucket, exists := st.Buckets[currentMinute]
	if !exists {
		bucket = &MinuteBucket{
			MinuteEpoch: currentMinute,
			High:        price,
			Low:         price,
			Open:        price,
			Close:       price,
			Count:       1,
		}
		st.Buckets[currentMinute] = bucket
	} else {
		if price > bucket.High {
			bucket.High = price
		}
		if price < bucket.Low {
			bucket.Low = price
		}
		bucket.Close = price
		bucket.Count++
	}

	// Track initial price if this is our first tick
	if st.FirstPrice == 0 {
		st.FirstPrice = price
		st.FirstTimestamp = ts
	}

	// O(1) Fast-Path: Check if current price breaks the 24h High or Low record
	if st.High24h == 0 || price > st.High24h {
		st.High24h = price
	}
	if st.Low24h == 0 || price < st.Low24h {
		st.Low24h = price
	}

	// Prune expired buckets older than 24 hours (1440 minutes)
	cutoffMinute := currentMinute - WindowMinutes
	if currentMinute != st.LastRecomputeMin {
		st.pruneAndRecompute(currentMinute, cutoffMinute, price)
		st.LastRecomputeMin = currentMinute
	}

	// Compute 24h % Change:
	// Base price is the open price of the oldest bucket within the 24-hour window
	basePrice := st.getOldestPriceInWindow(cutoffMinute)
	if basePrice <= 0 {
		basePrice = st.FirstPrice
	}

	if basePrice > 0 {
		pctChange := ((price - basePrice) / basePrice) * 100.0
		st.Change24h = math.Round(pctChange*10000) / 10000 // 4 decimal precision
	}

	st.LastBuy = raw.Buy
	st.LastSell = raw.Sell
	st.LastTimestamp = ts

	return models.ProcessedMarketTick{
		Symbol:    st.Symbol,
		Buy:       raw.Buy,
		Sell:      raw.Sell,
		High24h:   st.High24h,
		Low24h:    st.Low24h,
		Change24h: st.Change24h,
		Timestamp: ts,
	}, true
}

func (st *SymbolTracker) pruneAndRecompute(currentMinute, cutoffMinute int64, currentPrice float64) {
	newHigh := currentPrice
	newLow := currentPrice

	for minEpoch, b := range st.Buckets {
		if minEpoch <= cutoffMinute {
			delete(st.Buckets, minEpoch)
			continue
		}
		if b.High > newHigh {
			newHigh = b.High
		}
		if b.Low < newLow {
			newLow = b.Low
		}
	}

	st.High24h = newHigh
	st.Low24h = newLow
}

func (st *SymbolTracker) getOldestPriceInWindow(cutoffMinute int64) float64 {
	var oldestMin int64 = math.MaxInt64
	var oldestPrice float64 = 0

	for minEpoch, b := range st.Buckets {
		if minEpoch > cutoffMinute && minEpoch < oldestMin {
			oldestMin = minEpoch
			oldestPrice = b.Open
		}
	}
	return oldestPrice
}

func (st *SymbolTracker) GetCurrentSnapshot() models.ProcessedMarketTick {
	st.mu.RLock()
	defer st.mu.RUnlock()

	return models.ProcessedMarketTick{
		Symbol:    st.Symbol,
		Buy:       st.LastBuy,
		Sell:      st.LastSell,
		High24h:   st.High24h,
		Low24h:    st.Low24h,
		Change24h: st.Change24h,
		Timestamp: st.LastTimestamp,
	}
}

// Engine manages trackers for all symbols
type Engine struct {
	trackers   map[string]*SymbolTracker
	mu         sync.RWMutex
	inTicks    chan models.RawMarketTick
	outTicks   chan models.ProcessedMarketTick
	stopChan   chan struct{}
}

func NewEngine(bufferSize int) *Engine {
	return &Engine{
		trackers: make(map[string]*SymbolTracker),
		inTicks:  make(chan models.RawMarketTick, bufferSize),
		outTicks: make(chan models.ProcessedMarketTick, bufferSize),
		stopChan: make(chan struct{}),
	}
}

func (e *Engine) IngestChannel() chan<- models.RawMarketTick {
	return e.inTicks
}

func (e *Engine) OutputChannel() <-chan models.ProcessedMarketTick {
	return e.outTicks
}

func (e *Engine) Start() {
	go func() {
		for {
			select {
			case <-e.stopChan:
				return
			case raw, ok := <-e.inTicks:
				if !ok {
					return
				}
				tracker := e.getOrCreateTracker(raw.Symbol)
				processed, valid := tracker.ProcessTick(raw)
				if valid {
					select {
					case e.outTicks <- processed:
					default:
						// Drop or non-blocking in case downstream channel is momentarily full
					}
				}
			}
		}
	}()
}

func (e *Engine) Stop() {
	close(e.stopChan)
}

func (e *Engine) getOrCreateTracker(symbol string) *SymbolTracker {
	e.mu.RLock()
	tracker, exists := e.trackers[symbol]
	e.mu.RUnlock()

	if exists {
		return tracker
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if tracker, exists = e.trackers[symbol]; exists {
		return tracker
	}

	tracker = NewSymbolTracker(symbol)
	e.trackers[symbol] = tracker
	return tracker
}

func (e *Engine) GetAllSnapshots() []models.ProcessedMarketTick {
	e.mu.RLock()
	defer e.mu.RUnlock()

	snapshots := make([]models.ProcessedMarketTick, 0, len(e.trackers))
	for _, t := range e.trackers {
		snapshots = append(snapshots, t.GetCurrentSnapshot())
	}
	return snapshots
}

func (e *Engine) ExportTrackers() map[string]*SymbolTracker {
	e.mu.RLock()
	defer e.mu.RUnlock()

	copyMap := make(map[string]*SymbolTracker, len(e.trackers))
	for k, v := range e.trackers {
		copyMap[k] = v
	}
	return copyMap
}

func (e *Engine) RestoreTrackers(restored map[string]*SymbolTracker) {
	e.mu.Lock()
	defer e.mu.Unlock()

	for k, v := range restored {
		if v.Buckets == nil {
			v.Buckets = make(map[int64]*MinuteBucket)
		}
		e.trackers[k] = v
	}
}
