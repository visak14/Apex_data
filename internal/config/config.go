package config

import (
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

type Config struct {
	UpstreamWSURL            string
	APIKey                   string
	SecretKey                string
	ServerPort               string
	PersistenceFile          string
	SnapshotIntervalSeconds  int
	BroadcastIntervalMs      int
	EnableMockFallback       bool
	Symbols                  []string
}

var DefaultSymbols = []string{
	"AUDUSD", "AUDCAD", "AUDCHF", "AUDJPY", "AUDNZD",
	"AUDSGD", "CADCHF", "CADJPY", "CHFJPY", "CHFSGD",
	"EURAUD", "EURCAD", "EURCHF", "EURGBP", "EURJPY",
	"EURNZD", "EURUSD", "GBPAUD", "GBPCAD", "GBPCHF",
	"GBPJPY", "GBPNZD", "GBPUSD", "NZDCAD", "NZDCHF",
	"NZDJPY", "NZDUSD", "USDCAD", "USDCHF", "USDCNH",
	"USDJPY", "USDMXN", "USDNOK", "USDPLN", "USDSEK",
	"USDSGD", "USDTRY", "USDZAR", "EURTRY", "GBPTRY",
	"NOKJPY", "SEKJPY", "SGDJPY", "ZARJPY", "XAUUSD",
	"XAGUSD", "BTCUSD", "ETHUSD", "BNBUSD", "SOLUSD",
	"XRPUSD", "ADAUSD", "DOGUSD", "DOTUSD", "LTCUSD",
	"BCHUSD", "XLMUSD", "TRXUSD", "UNIUSD", "FILUSD",
	"AVXUSD",
}

func LoadConfig() *Config {
	_ = godotenv.Load()

	wsURL := getEnv("UPSTREAM_WS_URL", "wss://v3-prod.livefxhub.com/ws/algo")
	apiKey := getEnv("API_KEY", "50558cc472f332b399c2760009728e70f39562a4399693b5")
	secretKey := getEnv("SECRET_KEY", "2451125110c1a9216219b0aca76eb1d25380dba220d80e741100386c0dc1d31d")
	port := getEnv("PORT", "8080")
	persistFile := getEnv("PERSISTENCE_FILE", "data/market_state.json")

	snapInt, _ := strconv.Atoi(getEnv("SNAPSHOT_INTERVAL_SECONDS", "10"))
	if snapInt <= 0 {
		snapInt = 10
	}

	bcastInt, _ := strconv.Atoi(getEnv("BROADCAST_INTERVAL_MS", "30")) // 30ms (< 50ms latency requirement)
	if bcastInt <= 0 {
		bcastInt = 30
	}

	mockFallback := getEnv("ENABLE_MOCK_FALLBACK", "true") == "true"

	var symbols []string
	customSymbols := os.Getenv("SYMBOLS")
	if customSymbols != "" {
		for _, s := range strings.Split(customSymbols, ",") {
			trimmed := strings.TrimSpace(s)
			if trimmed != "" {
				symbols = append(symbols, trimmed)
			}
		}
	}
	if len(symbols) == 0 {
		symbols = DefaultSymbols
	}

	return &Config{
		UpstreamWSURL:           wsURL,
		APIKey:                  apiKey,
		SecretKey:               secretKey,
		ServerPort:              port,
		PersistenceFile:         persistFile,
		SnapshotIntervalSeconds: snapInt,
		BroadcastIntervalMs:     bcastInt,
		EnableMockFallback:      mockFallback,
		Symbols:                 symbols,
	}
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}
