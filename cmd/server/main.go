package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"market-data-engine/internal/config"
	"market-data-engine/internal/downstream"
	"market-data-engine/internal/processor"
	"market-data-engine/internal/storage"
	"market-data-engine/internal/upstream"
)

func main() {
	log.Println("==================================================")
	log.Println("  Low-Latency Market Data Processing Engine v1.0  ")
	log.Println("==================================================")

	// 1. Load Config
	cfg := config.LoadConfig()
	log.Printf("[Config] Tracking %d market symbols", len(cfg.Symbols))
	log.Printf("[Config] Upstream Feed: %s", cfg.UpstreamWSURL)
	log.Printf("[Config] Downstream Server Port: :%s", cfg.ServerPort)

	// 2. Initialize Engine
	engine := processor.NewEngine(10000)
	engine.Start()

	// 3. Initialize State Persistence Manager
	storageMgr := storage.NewPersistenceManager(cfg.PersistenceFile, cfg.SnapshotIntervalSeconds, engine)
	storageMgr.Start()

	// 4. Initialize Downstream WebSocket Server
	downstreamServer := downstream.NewServer(cfg.ServerPort, engine, cfg.BroadcastIntervalMs)
	downstreamServer.Start()

	// 5. Connect Output of Engine to Downstream Server
	go func() {
		for tick := range engine.OutputChannel() {
			downstreamServer.BroadcastTick(tick)
		}
	}()

	// 6. Initialize Upstream Client
	upstreamClient := upstream.NewUpstreamClient(cfg, engine.IngestChannel())
	upstreamClient.Start()

	// 7. Wait for OS Termination Signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	<-sigChan
	log.Println("\n[Main] Shutdown signal received. Performing graceful shutdown...")

	upstreamClient.Stop()
	engine.Stop()
	storageMgr.Stop() // Flush state to disk

	log.Println("[Main] State safely persisted. Engine terminated gracefully.")
	time.Sleep(500 * time.Millisecond)
}
