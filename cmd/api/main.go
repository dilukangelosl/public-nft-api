package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/dilukangelosl/public-nft-api/config"
	"github.com/dilukangelosl/public-nft-api/internal/api"
	"github.com/dilukangelosl/public-nft-api/internal/cache"
	"github.com/dilukangelosl/public-nft-api/internal/chain"
	"github.com/dilukangelosl/public-nft-api/internal/indexer"
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

	// Wait for DB (we only run if healthy)
	ctx := context.Background()
	db, err := store.NewStore(ctx, cfg.DatabaseURL)
	if err != nil {
		logger.Fatal("Failed connecting to DB", zap.Error(err))
	}
	defer db.Close()

	// Wait for Alchemy HTTP/WSS RPC
	eth, err := chain.Connect(ctx, cfg.AlchemyWSS, cfg.AlchemyHTTP)
	if err != nil {
		logger.Fatal("Failed connecting to Ethereum RPC", zap.Error(err))
	}

	// Cache
	c, err := cache.NewCache(logger)
	if err != nil {
		logger.Fatal("Failed creating cache", zap.Error(err))
	}

	// Setup Shared Indexer Queues so the API can manually push Collections & Tokens
	mdQueue := make(map[string]chan string)
	mdMutex := &sync.RWMutex{}
	
	// Create listener (but we don't 'Start' it as a background daemon, we just use it for manual inject queues)
	listener := indexer.NewListener(eth, db, logger, mdQueue, mdMutex)

	port := cfg.Port
	if port == "" {
		port = "8080"
	}
	serverAddr := fmt.Sprintf("0.0.0.0:%s", port)

	srv := &http.Server{
		Addr:    serverAddr,
		Handler: api.NewServer(db, listener, c, logger, mdMutex, mdQueue),
	}

	go func() {
		logger.Info("Starting Public REST API", zap.String("port", port))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("listen: %s\n", zap.Error(err))
		}
	}()

	// Graceful Setup Shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit

	logger.Info("Attempting graceful shutdown...")

	ctxShutDown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctxShutDown); err != nil {
		logger.Fatal("Server forced shutting down", zap.Error(err))
	}

	logger.Info("Server exiting gracefully.")
}
