package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"

	"github.com/dilukangelosl/public-nft-api/internal/indexer"
	"github.com/dilukangelosl/public-nft-api/internal/store"
	"github.com/go-chi/chi/v5"
	"go.uber.org/zap"
)

type API struct {
	Store         *store.Store
	Listener      *indexer.Listener
	Logger        *zap.Logger
	MetadataMutex *sync.RWMutex
	MetadataQueue map[string]chan string
	ChainName     string
}

// POST /v1/collections
func (a *API) HandleQueueCollection(w http.ResponseWriter, r *http.Request) {
	var payload struct {
		Address string `json:"address"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		RespondError(w, http.StatusBadRequest, "invalid_payload", "Failed to parse JSON body")
		return
	}

	addr := strings.ToLower(strings.TrimSpace(payload.Address))
	if len(addr) != 42 || !strings.HasPrefix(addr, "0x") {
		RespondError(w, http.StatusBadRequest, "invalid_address", "Invalid Ethereum address format")
		return
	}

	// Fast path check database directly
	if coll, err := a.Store.GetCollection(r.Context(), addr); err == nil {
		RespondJSON(w, http.StatusOK, APIResponse{Data: coll})
		return
	}

	// Trigger enqueue using PostgreSQL NOTIFY IPC
	_, err := a.Store.Pool.Exec(r.Context(), "SELECT pg_notify('manual_discovery', $1)", addr)
	if err != nil {
		a.Logger.Error("Failed to broadcast manual discovery notification", zap.Error(err))
		RespondError(w, http.StatusInternalServerError, "notification_failed", "Failed to queue collection")
		return
	}

	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte(`{"message": "Collection queued for discovery and indexing."}`))
}

// GET /v1/collections/:contract
func (a *API) HandleGetCollection(w http.ResponseWriter, r *http.Request) {
	contract := chi.URLParam(r, "contract")

	coll, err := a.Store.GetCollection(r.Context(), contract)
	if err != nil {
		RespondError(w, http.StatusNotFound, "not_found", "Collection not found")
		return
	}

	RespondJSON(w, http.StatusOK, APIResponse{Data: coll})
}

// POST /v1/collections/:contract/reindex-metadata
func (a *API) HandleReindexMetadata(w http.ResponseWriter, r *http.Request) {
	contract := chi.URLParam(r, "contract")

	coll, err := a.Store.GetCollection(r.Context(), contract)
	if err != nil {
		RespondError(w, http.StatusNotFound, "not_found", "Collection not found")
		return
	}

	if coll.Reindexing {
		RespondError(w, http.StatusConflict, "reindexing", "Metadata reindex already in progress for this collection")
		return
	}

	// Initiate background reindex routine
	go a.triggerFullReindex(context.Background(), contract, coll.TotalSupply)

	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte(`{"message": "Full collection metadata re-queued for fetch."}`))
}

func (a *API) triggerFullReindex(ctx context.Context, contract string, totalSupply int64) {
	// 1. Mark reindexing true globally
	_ = a.Store.SetCollectionReindexing(ctx, contract, true)
	defer a.Store.SetCollectionReindexing(ctx, contract, false) // un-flag when done generating jobs

	// 2. Clear fetched status for the whole contract
	if err := a.Store.ResetCollectionMetadataStatus(ctx, contract); err != nil {
		a.Logger.Error("Failed to reset metadata fetch status", zap.Error(err))
		return
	}

	// 3. Flood the metadata queue with jobs
	a.MetadataMutex.Lock()
	if _, ok := a.MetadataQueue[contract]; !ok {
		// re-init channel bound if it naturally decayed
		bound := totalSupply
		if bound > 50000 {
			bound = 50000
		}
		a.MetadataQueue[contract] = make(chan string, bound)
	}
	a.MetadataMutex.Unlock()

	// 4. Send all IDs from DB into Queue.
	// Production NOTE: For 10k items, simple loop queries block. For true scale, a cursor streaming rows down is needed.
	// For this spec, sending 1 by 1 from standard querying works.
	rows, err := a.Store.Pool.Query(ctx, "SELECT token_id FROM tokens WHERE contract = $1", contract)
	if err != nil {
		a.Logger.Error("Failed to fetch token list for reindex", zap.Error(err))
		return
	}
	defer rows.Close()

	ch := a.MetadataQueue[contract]
	for rows.Next() {
		var tokenID string
		if err := rows.Scan(&tokenID); err == nil {
			ch <- tokenID
		}
	}
}

// POST /v1/collections/:contract/tokens/:tokenId/reindex-metadata
func (a *API) HandleReindexSingleToken(w http.ResponseWriter, r *http.Request) {
	contract := chi.URLParam(r, "contract")
	tokenID := chi.URLParam(r, "tokenId")

	// Set fetch state to false internally directly against token row
	_, err := a.Store.Pool.Exec(r.Context(), "UPDATE tokens SET metadata_fetched = false WHERE contract = $1 AND token_id = $2", contract, tokenID)
	if err != nil {
		RespondError(w, http.StatusNotFound, "not_found", "Token not found")
		return
	}

	// Inject back into active pool
	a.MetadataMutex.RLock()
	if ch, ok := a.MetadataQueue[contract]; ok {
		select {
		case ch <- tokenID:
		default:
		}
	} else {
		// if channel decayed, recreate manually to wake up dispatcher
		a.MetadataMutex.RUnlock()
		a.MetadataMutex.Lock()
		a.MetadataQueue[contract] = make(chan string, 100)
		a.MetadataQueue[contract] <- tokenID
		a.MetadataMutex.Unlock()
		a.MetadataMutex.RLock()
	}
	a.MetadataMutex.RUnlock()

	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte(`{"message": "Single token queued for metadata fetch."}`))
}
