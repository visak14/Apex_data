package processor

import (
	"testing"
	"time"

	"market-data-engine/internal/models"
)

func TestRollingWindowCalculations(t *testing.T) {
	st := NewSymbolTracker("BTCUSD")
	now := time.Now().UnixMilli()

	// 1. First tick at price 100
	tick1 := models.RawMarketTick{
		Symbol:    "BTCUSD",
		Buy:       100.0,
		Sell:      100.0,
		Timestamp: now,
		// Upstream values that MUST be ignored
		Low24h:    9999.0,
		High24h:   1.0,
		Change24h: 50.0,
	}

	p1, ok := st.ProcessTick(tick1)
	if !ok {
		t.Fatalf("expected tick1 to process successfully")
	}
	if p1.High24h != 100.0 || p1.Low24h != 100.0 {
		t.Errorf("expected high/low to be 100, got high=%f, low=%f", p1.High24h, p1.Low24h)
	}
	if p1.Change24h != 0.0 {
		t.Errorf("expected 0 change on first tick, got %f", p1.Change24h)
	}

	// 2. Duplicate tick (same timestamp and prices) -> should be dropped
	_, okDup := st.ProcessTick(tick1)
	if okDup {
		t.Errorf("expected duplicate tick to be dropped")
	}

	// 3. Second tick price moves to 120 (1 minute later)
	tick2 := models.RawMarketTick{
		Symbol:    "BTCUSD",
		Buy:       120.0,
		Sell:      120.0,
		Timestamp: now + 60000,
	}
	p2, ok2 := st.ProcessTick(tick2)
	if !ok2 {
		t.Fatalf("expected tick2 to process")
	}
	if p2.High24h != 120.0 {
		t.Errorf("expected High24h=120, got %f", p2.High24h)
	}
	if p2.Low24h != 100.0 {
		t.Errorf("expected Low24h=100, got %f", p2.Low24h)
	}
	// Change should be ((120 - 100) / 100) * 100 = +20%
	if p2.Change24h != 20.0 {
		t.Errorf("expected Change24h=+20%%, got %f", p2.Change24h)
	}

	// 4. Third tick price drops to 80
	tick3 := models.RawMarketTick{
		Symbol:    "BTCUSD",
		Buy:       80.0,
		Sell:      80.0,
		Timestamp: now + 120000,
	}
	p3, ok3 := st.ProcessTick(tick3)
	if !ok3 {
		t.Fatalf("expected tick3 to process")
	}
	if p3.High24h != 120.0 {
		t.Errorf("expected High24h=120, got %f", p3.High24h)
	}
	if p3.Low24h != 80.0 {
		t.Errorf("expected Low24h=80, got %f", p3.Low24h)
	}
	// Change should be ((80 - 100) / 100) * 100 = -20%
	if p3.Change24h != -20.0 {
		t.Errorf("expected Change24h=-20%%, got %f", p3.Change24h)
	}
}
