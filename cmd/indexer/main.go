package main

import (
	"context"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/dilukangelosl/public-nft-api/config"
	"github.com/dilukangelosl/public-nft-api/internal/chain"
	"github.com/dilukangelosl/public-nft-api/internal/indexer"
	"github.com/dilukangelosl/public-nft-api/internal/metadata"
	"github.com/dilukangelosl/public-nft-api/internal/store"
	"go.uber.org/zap"
)

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	cfg, err := config.Load()
	if err != nil {
		logger.Fatal("Failed reading config", zap.Error(err))
	}
	
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := store.NewStore(ctx, cfg.DatabaseURL)
	if err != nil {
		logger.Fatal("Failed connecting to DB", zap.Error(err))
	}
	defer db.Close()

	eth, err := chain.Connect(ctx, cfg.AlchemyWSS, cfg.AlchemyHTTP)
	if err != nil {
		logger.Fatal("Failed connecting to Ethereum RPC", zap.Error(err))
	}

	gateways := cfg.IPFSGateways
	for i := range gateways {
		gateways[i] = strings.TrimSpace(gateways[i])
	}
	logger.Info("Active IPFS gateways setup initialized", zap.Int("count", len(gateways)))

	// Core Architecture Wiring
	mdQueue := make(map[string]chan string)
	mdMutex := &sync.RWMutex{}

	listener := indexer.NewListener(eth, db, logger, mdQueue, mdMutex)
	dispatcher := metadata.NewDispatcher(eth, db, logger, gateways)
	
	// Map internal tracking structure together 
	dispatcher.QueueMap = mdQueue
	dispatcher.Mutex = mdMutex

	// Startup Daemons
	go listener.Start(ctx)
	go dispatcher.Start(ctx)

	// Recovery: on startup, re-snapshot every known collection to catch ownership
	// drift that occurred while the indexer was offline. ForceResnap bypasses the
	// knownContracts guard and uses BulkUpsertTokens for idempotent writes.
	go func() {
		time.Sleep(3 * time.Second) // wait for WS subscription and LISTEN to be active
		all, err := db.GetAllCollections(ctx)
		if err != nil {
			logger.Error("Failed to query collections for startup recovery", zap.Error(err))
			return
		}
		if len(all) > 0 {
			logger.Info("Startup recovery: re-snapshotting all collections", zap.Int("count", len(all)))
			for _, c := range all {
				listener.ForceResnap(c.Address)
			}
		}
	}()

	logger.Info("Indexer & Metadata Background Daemons Running Successfully")

	// Sig handling 
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit

	logger.Info("Shutdown requested. Terminating all tasks gracefully...")
	cancel() // Kill ctx which forces active goroutines scanning contexts to complete normally
	
	// Wait a moment before hard exit
	db.Close()
	logger.Info("DB connection killed.")
}
