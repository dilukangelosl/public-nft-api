package api

import (
	_ "embed"
	"bytes"
	"net/http"
	"sync"
	"time"

	"github.com/dilukangelosl/public-nft-api/internal/cache"
	"github.com/dilukangelosl/public-nft-api/internal/indexer"
	"github.com/dilukangelosl/public-nft-api/internal/store"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

func NewServer(
	store *store.Store,
	listener *indexer.Listener,
	cacheSvc *cache.Cache,
	logger *zap.Logger,
	mdMutex *sync.RWMutex,
	mdQueue map[string]chan string,
	chainName string,
) http.Handler {

	api := &API{
		Store:         store,
		Listener:      listener,
		Logger:        logger,
		MetadataMutex: mdMutex,
		MetadataQueue: mdQueue,
		ChainName:     chainName,
	}

	r := chi.NewRouter()

	// Default chi middleware
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(60 * time.Second))

	// CORS Policy
	r.Use(corsMiddleware)

	// Rate Limiting (60 requests per minute per IP)
	limiter := NewIPRateLimiter(rate.Every(time.Second), 60)
	r.Use(RateLimitMiddleware(limiter))

	// Base namespace
	r.Route("/v1", func(r chi.Router) {
		
		// Uncached Health / Status Routes
		r.Get("/health", api.HandleHealth)
		
		// Command/Enqueuing POST endpoints
		r.Post("/collections", api.HandleQueueCollection)
		r.Post("/collections/{contract}/reindex-metadata", api.HandleReindexMetadata)
		r.Post("/collections/{contract}/tokens/{tokenId}/reindex-metadata", api.HandleReindexSingleToken)

		// Cached Data Retrieval GET endpoints
		r.Group(func(cr chi.Router) {
			cr.Use(CacheMiddleware(cacheSvc, 30*time.Second)) // cache GET lists for 30s
			
			cr.Get("/collections/{contract}", api.HandleGetCollection)
			cr.Get("/collections/{contract}/tokens", api.HandleListTokens)
			cr.Get("/collections/{contract}/tokens/{tokenId}", api.HandleGetToken)
			
			cr.Get("/collections/{contract}/owners", api.HandleGetCollectionOwners)
			cr.Get("/owners/{address}", api.HandleGetOwnerTokens)
			cr.Get("/owners/{address}/collections/{contract}", api.HandleGetOwnerCollectionTokens)
		})
	})

	// Setup basic Developer API landing page mapping
	r.Get("/", LandingPageHandler(api.ChainName))

	return r
}

//go:embed static/index.html
var indexHTML []byte

func LandingPageHandler(chainName string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		page := bytes.ReplaceAll(indexHTML, []byte("{{CHAIN_NAME}}"), []byte(chainName))
		w.Write(page)
	}
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}
