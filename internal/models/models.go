package models

// RawMarketTick represents the incoming JSON packet from LiveFXHub upstream
type RawMarketTick struct {
	Symbol     string  `json:"symbol"`
	Buy        float64 `json:"buy"`
	Sell       float64 `json:"sell"`
	Low24h     float64 `json:"low_24h,omitempty"`     // MUST BE IGNORED per assignment
	High24h    float64 `json:"high_24h,omitempty"`    // MUST BE IGNORED per assignment
	Change24h  float64 `json:"change_24h,omitempty"`  // MUST BE IGNORED per assignment
	Timestamp  int64   `json:"timestamp"`             // Millisecond epoch
}

// ProcessedMarketTick represents the output tick after our own rolling 24h computation
type ProcessedMarketTick struct {
	Symbol      string  `json:"symbol"`
	Buy         float64 `json:"buy"`
	Sell        float64 `json:"sell"`
	High24h     float64 `json:"high_24h"`             // Computed by our engine
	Low24h      float64 `json:"low_24h"`              // Computed by our engine
	Change24h   float64 `json:"change_24h"`           // Computed by our engine (% change)
	Timestamp   int64   `json:"timestamp"`
	LatencyMs   float64 `json:"latency_ms,omitempty"` // Ingress to Egress latency
}

// CompactMarketTick is an ultra-dense format to guarantee < 10KB payload
type CompactMarketTick struct {
	S string  `json:"s"` // Symbol
	B float64 `json:"b"` // Buy (Bid)
	A float64 `json:"a"` // Sell (Ask)
	H float64 `json:"h"` // 24h High
	L float64 `json:"l"` // 24h Low
	C float64 `json:"c"` // 24h % Change
	T int64   `json:"t"` // Timestamp
}

// ToCompact converts a ProcessedMarketTick to CompactMarketTick
func (p *ProcessedMarketTick) ToCompact() CompactMarketTick {
	return CompactMarketTick{
		S: p.Symbol,
		B: p.Buy,
		A: p.Sell,
		H: p.High24h,
		L: p.Low24h,
		C: p.Change24h,
		T: p.Timestamp,
	}
}

// ClientMessage represents client incoming subscription requests
type ClientMessage struct {
	Action  string   `json:"action"`            // e.g. "subscribe", "unsubscribe"
	Symbols []string `json:"symbols,omitempty"` // List of symbols or ["*"]
	Format  string   `json:"format,omitempty"`  // "full" or "compact" (default compact)
}

// ServerBroadcastMessage is what the custom WebSocket server sends to downstream clients
type ServerBroadcastMessage struct {
	Type        string               `json:"type"`                   // "snapshot", "update", "delta"
	Count       int                  `json:"count"`                  // Number of symbols updated
	Data        []ProcessedMarketTick`json:"data,omitempty"`         // Standard format
	CompactData []CompactMarketTick  `json:"compact,omitempty"`      // Compact format
	PayloadKB   float64              `json:"payload_kb"`             // Exact payload size in KB
	ServerTime  int64                `json:"server_time"`
	LatencyMs   float64              `json:"latency_ms,omitempty"`   // End-to-end processing time
}
