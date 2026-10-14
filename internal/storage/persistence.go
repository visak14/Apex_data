package storage

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"market-data-engine/internal/processor"
)

type PersistenceManager struct {
	filePath        string
	intervalSeconds int
	engine          *processor.Engine
	stopChan        chan struct{}
	mu              sync.Mutex
}

func NewPersistenceManager(filePath string, intervalSec int, engine *processor.Engine) *PersistenceManager {
	return &PersistenceManager{
		filePath:        filePath,
		intervalSeconds: intervalSec,
		engine:          engine,
		stopChan:        make(chan struct{}),
	}
}

// Start begins the periodic background snapshot timer
func (pm *PersistenceManager) Start() {
	// 1. Ensure directory exists
	dir := filepath.Dir(pm.filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Printf("[Storage] Warning: unable to create storage dir %s: %v", dir, err)
	}

	// 2. Restore previous state if file exists
	pm.Restore()

	// 3. Periodic flusher
	go func() {
		ticker := time.NewTicker(time.Duration(pm.intervalSeconds) * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-pm.stopChan:
				pm.Save() // Final snapshot on shutdown
				return
			case <-ticker.C:
				if err := pm.Save(); err != nil {
					log.Printf("[Storage] Snapshot error: %v", err)
				}
			}
		}
	}()
}

func (pm *PersistenceManager) Stop() {
	close(pm.stopChan)
}

// Save atomically writes engine state to disk
func (pm *PersistenceManager) Save() error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	trackers := pm.engine.ExportTrackers()
	if len(trackers) == 0 {
		return nil
	}

	data, err := json.Marshal(trackers)
	if err != nil {
		return fmt.Errorf("failed to marshal state: %w", err)
	}

	// Write to temporary file first to guarantee atomic write
	tmpFile := pm.filePath + ".tmp"
	if err := os.WriteFile(tmpFile, data, 0644); err != nil {
		return fmt.Errorf("failed to write tmp file: %w", err)
	}

	// Atomic replace
	if err := os.Rename(tmpFile, pm.filePath); err != nil {
		return fmt.Errorf("failed to rename tmp file: %w", err)
	}

	return nil
}

// Restore loads previously saved state into engine
func (pm *PersistenceManager) Restore() {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if _, err := os.Stat(pm.filePath); os.IsNotExist(err) {
		log.Printf("[Storage] No existing state file found at %s. Starting fresh.", pm.filePath)
		return
	}

	data, err := os.ReadFile(pm.filePath)
	if err != nil {
		log.Printf("[Storage] Failed to read state file: %v", err)
		return
	}

	var restored map[string]*processor.SymbolTracker
	if err := json.Unmarshal(data, &restored); err != nil {
		log.Printf("[Storage] Failed to unmarshal state file: %v", err)
		return
	}

	pm.engine.RestoreTrackers(restored)
	log.Printf("[Storage] Successfully restored %d symbol states from disk.", len(restored))
}
