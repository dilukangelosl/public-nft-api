package api

import (
	"context"
	"encoding/json"
	"fmt"
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
// Catch-up: re-queues only tokens with metadata_fetched=false (pending tokens).
func (a *API) HandleReindexMetadata(w http.ResponseWriter, r *http.Request) {
	contract := chi.URLParam(r, "contract")
	coll, err := a.Store.GetCollection(r.Context(), contract)
	if err != nil {
		RespondError(w, http.StatusNotFound, "not_found", "Collection not found")
		return
	}
	if coll.Reindexing {
		RespondError(w, http.StatusConflict, "reindexing", "Metadata reindex already in progress")
		return
	}
	go a.triggerFullReindex(context.Background(), contract, coll.TotalSupply, false)
	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte(`{"message": "Pending metadata tokens re-queued."}`))
}

// POST /v1/collections/:contract/resync-metadata
// Force-refresh ALL tokens including already-fetched (for dynamic NFTs).
// ?delete_stale=true also wipes existing metadata rows before re-fetching.
func (a *API) HandleResyncMetadata(w http.ResponseWriter, r *http.Request) {
	contract := chi.URLParam(r, "contract")
	deleteStale := r.URL.Query().Get("delete_stale") == "true"

	coll, err := a.Store.GetCollection(r.Context(), contract)
	if err != nil {
		RespondError(w, http.StatusNotFound, "not_found", "Collection not found")
		return
	}
	if coll.Reindexing {
		RespondError(w, http.StatusConflict, "reindexing", "Metadata resync already in progress")
		return
	}

	if deleteStale {
		if _, err := a.Store.Pool.Exec(r.Context(), `DELETE FROM metadata WHERE contract = $1`, contract); err != nil {
			a.Logger.Error("Failed to delete stale metadata", zap.String("contract", contract), zap.Error(err))
			RespondError(w, http.StatusInternalServerError, "db_error", "Failed to clear stale metadata")
			return
		}
	}

	go a.triggerFullReindex(context.Background(), contract, coll.TotalSupply, true)
	msg := `{"message": "Full metadata resync queued for all tokens."}`
	if deleteStale {
		msg = `{"message": "Stale metadata cleared. Full resync queued for all tokens."}`
	}
	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte(msg))
}


// triggerFullReindex queues all tokens for metadata fetch.
// force=true: resets metadata_fetched=false for ALL tokens (including already-fetched).
// force=false: only re-queues tokens already marked as metadata_fetched=false (catch-up).
func (a *API) triggerFullReindex(ctx context.Context, contract string, totalSupply int64, force bool) {
	_ = a.Store.SetCollectionReindexing(ctx, contract, true)
	defer a.Store.SetCollectionReindexing(ctx, contract, false)

	if force {
		// Reset ALL tokens so dispatcher will re-fetch every one
		if _, err := a.Store.Pool.Exec(ctx, `UPDATE tokens SET metadata_fetched = false WHERE contract = $1`, contract); err != nil {
			a.Logger.Error("Failed to reset all metadata flags", zap.Error(err))
			return
		}
	} else {
		// Legacy: only reset the ones stuck as not-fetched
		if err := a.Store.ResetCollectionMetadataStatus(ctx, contract); err != nil {
			a.Logger.Error("Failed to reset pending metadata flags", zap.Error(err))
			return
		}
	}

	// Ensure the metadata queue channel exists
	a.MetadataMutex.Lock()
	if _, ok := a.MetadataQueue[contract]; !ok {
		bound := totalSupply
		if bound > 50000 {
			bound = 50000
		}
		a.MetadataQueue[contract] = make(chan string, bound)
	}
	a.MetadataMutex.Unlock()

	// Stream all token IDs into the queue
	query := `SELECT token_id FROM tokens WHERE contract = $1`
	if !force {
		query = `SELECT token_id FROM tokens WHERE contract = $1 AND metadata_fetched = false`
	}
	rows, err := a.Store.Pool.Query(ctx, query, contract)
	if err != nil {
		a.Logger.Error("Failed to fetch token list for reindex", zap.Error(err))
		return
	}
	defer rows.Close()

	ch := a.MetadataQueue[contract]
	for rows.Next() {
		var tokenID string
		if err := rows.Scan(&tokenID); err == nil {
			select {
			case ch <- tokenID:
			default:
				// Channel full — dispatcher will drain before we can push more
				a.Logger.Debug("Metadata queue full during reindex", zap.String("contract", contract), zap.String("token", tokenID))
			}
		}
	}
}

// POST /v1/collections/:contract/tokens/:tokenId/reindex-metadata
// Catches up a single token's metadata if it was never fetched.
func (a *API) HandleReindexSingleToken(w http.ResponseWriter, r *http.Request) {
	a.enqueueSingleTokenMetadata(w, r, false)
}

// POST /v1/collections/:contract/tokens/:tokenId/resync-metadata
// Force-refreshes a single token's metadata even if already fetched (for dynamic NFTs).
func (a *API) HandleResyncSingleToken(w http.ResponseWriter, r *http.Request) {
	a.enqueueSingleTokenMetadata(w, r, true)
}

func (a *API) enqueueSingleTokenMetadata(w http.ResponseWriter, r *http.Request, force bool) {
	contract := chi.URLParam(r, "contract")
	tokenID := chi.URLParam(r, "tokenId")

	if force {
		// Delete existing metadata row so it's re-fetched fresh
		_, _ = a.Store.Pool.Exec(r.Context(), `DELETE FROM metadata WHERE contract = $1 AND token_id = $2`, contract, tokenID)
	}

	// Reset token's metadata_fetched flag
	res, err := a.Store.Pool.Exec(r.Context(), `UPDATE tokens SET metadata_fetched = false WHERE contract = $1 AND token_id = $2`, contract, tokenID)
	if err != nil || res.RowsAffected() == 0 {
		RespondError(w, http.StatusNotFound, "not_found", "Token not found")
		return
	}

	// Push into the metadata queue
	a.MetadataMutex.RLock()
	ch, ok := a.MetadataQueue[contract]
	a.MetadataMutex.RUnlock()

	if !ok {
		a.MetadataMutex.Lock()
		a.MetadataQueue[contract] = make(chan string, 100)
		ch = a.MetadataQueue[contract]
		a.MetadataMutex.Unlock()
	}

	select {
	case ch <- tokenID:
	default:
	}

	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte(`{"message": "Token queued for metadata fetch."}`))
}

// POST /v1/metadata/retry-failed
// Global endpoint to fetch all failed metadata and queue them for refetching.
func (a *API) HandleRetryFailedMetadata(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()

	// Find all tokens that haven't had their metadata fetched (or failed previously)
	query := `SELECT contract, token_id FROM tokens WHERE metadata_fetched = false`
	rows, err := a.Store.Pool.Query(ctx, query)
	if err != nil {
		a.Logger.Error("Failed to query failed/pending metadata", zap.Error(err))
		RespondError(w, http.StatusInternalServerError, "db_error", "Failed to query pending metadata")
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var contract, tokenID string
		if err := rows.Scan(&contract, &tokenID); err == nil {
			count++
			a.MetadataMutex.Lock()
			if _, ok := a.MetadataQueue[contract]; !ok {
				a.MetadataQueue[contract] = make(chan string, 10000)
			}
			ch := a.MetadataQueue[contract]
			a.MetadataMutex.Unlock()

			select {
			case ch <- tokenID:
			default:
				a.Logger.Debug("Metadata queue full during retry-failed", zap.String("contract", contract), zap.String("token", tokenID))
			}
		}
	}

	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte(fmt.Sprintf(`{"message": "Queued %d failed or pending tokens across all collections for metadata fetch."}`, count)))
}

