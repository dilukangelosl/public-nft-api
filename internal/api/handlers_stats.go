package api

import (
	"math"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"go.uber.org/zap"
)

type CollectionStats struct {
	Address          string          `json:"address"`
	Name             string          `json:"name"`
	Symbol           string          `json:"symbol"`
	TotalSupply      int64           `json:"total_supply"`
	SnapshotDone     bool            `json:"snapshot_done"`
	SnapshotBlock    int64           `json:"snapshot_block"`
	TokensIndexed    int64           `json:"tokens_indexed"`
	BurnedCount      int64           `json:"burned_count"`
	UniqueOwners     int64           `json:"unique_owners"`
	MetadataFetched  int64           `json:"metadata_fetched"`
	MetadataPending  int64           `json:"metadata_pending"`
	MetadataProgress float64         `json:"metadata_progress_pct"`
	RecentErrors     []MetadataError `json:"recent_errors,omitempty"`
}

type MetadataError struct {
	TokenID    string `json:"token_id"`
	Error      string `json:"error"`
	OccurredAt string `json:"occurred_at"`
}

func (a *API) loadRecentErrors(r *http.Request, contract string) []MetadataError {
	rows, err := a.Store.Pool.Query(r.Context(), `
		SELECT token_id, error, occurred_at
		FROM metadata_errors
		WHERE contract = $1
		ORDER BY occurred_at DESC
		LIMIT 20
	`, contract)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var errs []MetadataError
	for rows.Next() {
		var e MetadataError
		var t time.Time
		if err := rows.Scan(&e.TokenID, &e.Error, &t); err == nil {
			e.OccurredAt = t.UTC().Format(time.RFC3339)
			errs = append(errs, e)
		}
	}
	return errs
}


// GET /v1/collections/:contract/stats
// Returns snapshot status, token count, unique owners, burned count, and metadata progress.
func (a *API) HandleCollectionStats(w http.ResponseWriter, r *http.Request) {
	contract := chi.URLParam(r, "contract")

	var s CollectionStats
	var metaFetched int64

	err := a.Store.Pool.QueryRow(r.Context(), `
		SELECT
			c.address,
			c.name,
			c.symbol,
			c.total_supply,
			c.snapshot_done,
			c.snapshot_block,
			COUNT(t.token_id)                                             AS tokens_indexed,
			COUNT(t.token_id) FILTER (WHERE t.owner IN (
				'0x0000000000000000000000000000000000000000',
				'0x000000000000000000000000000000000000dEaD'
			))                                                            AS burned_count,
			COUNT(DISTINCT t.owner) FILTER (WHERE t.owner NOT IN (
				'0x0000000000000000000000000000000000000000',
				'0x000000000000000000000000000000000000dEaD'
			))                                                            AS unique_owners,
			COUNT(m.token_id)                                             AS metadata_fetched
		FROM collections c
		LEFT JOIN tokens  t ON t.contract = c.address
		LEFT JOIN metadata m ON m.contract = c.address AND m.token_id = t.token_id
		WHERE c.address = $1
		GROUP BY c.address, c.name, c.symbol, c.total_supply, c.snapshot_done, c.snapshot_block
	`, contract).Scan(
		&s.Address, &s.Name, &s.Symbol, &s.TotalSupply,
		&s.SnapshotDone, &s.SnapshotBlock,
		&s.TokensIndexed, &s.BurnedCount, &s.UniqueOwners,
		&metaFetched,
	)
	if err != nil {
		a.Logger.Error("collection stats query failed", zap.String("contract", contract), zap.Error(err))
		RespondError(w, http.StatusNotFound, "not_found", "Collection not found")
		return
	}

	s.MetadataFetched = metaFetched
	s.MetadataPending = s.TokensIndexed - metaFetched
	if s.TokensIndexed > 0 {
		s.MetadataProgress = math.Round(float64(metaFetched)/float64(s.TokensIndexed)*10000) / 100
	}
	s.RecentErrors = a.loadRecentErrors(r, contract)

	RespondJSON(w, http.StatusOK, APIResponse{Data: s})
}

// GET /v1/stats
// Returns all collections with indexing and metadata progress.
func (a *API) HandleGlobalStats(w http.ResponseWriter, r *http.Request) {
	type Row struct {
		CollectionStats
	}

	rows, err := a.Store.Pool.Query(r.Context(), `
		SELECT
			c.address,
			c.name,
			c.symbol,
			c.total_supply,
			c.snapshot_done,
			c.snapshot_block,
			COUNT(t.token_id)                                             AS tokens_indexed,
			COUNT(t.token_id) FILTER (WHERE t.owner IN (
				'0x0000000000000000000000000000000000000000',
				'0x000000000000000000000000000000000000dEaD'
			))                                                            AS burned_count,
			COUNT(DISTINCT t.owner) FILTER (WHERE t.owner NOT IN (
				'0x0000000000000000000000000000000000000000',
				'0x000000000000000000000000000000000000dEaD'
			))                                                            AS unique_owners,
			COUNT(m.token_id)                                             AS metadata_fetched
		FROM collections c
		LEFT JOIN tokens  t ON t.contract = c.address
		LEFT JOIN metadata m ON m.contract = c.address AND m.token_id = t.token_id
		GROUP BY c.address, c.name, c.symbol, c.total_supply, c.snapshot_done, c.snapshot_block
		ORDER BY c.total_supply DESC
	`)
	if err != nil {
		a.Logger.Error("global stats query failed", zap.Error(err))
		RespondError(w, http.StatusInternalServerError, "db_error", "Failed to query stats")
		return
	}
	defer rows.Close()

	var items []CollectionStats
	for rows.Next() {
		var s CollectionStats
		var metaFetched int64
		if err := rows.Scan(
			&s.Address, &s.Name, &s.Symbol, &s.TotalSupply,
			&s.SnapshotDone, &s.SnapshotBlock,
			&s.TokensIndexed, &s.BurnedCount, &s.UniqueOwners,
			&metaFetched,
		); err != nil {
			a.Logger.Error("failed to scan stats row", zap.Error(err))
			continue
		}
		s.MetadataFetched = metaFetched
		s.MetadataPending = s.TokensIndexed - metaFetched
		if s.TokensIndexed > 0 {
			s.MetadataProgress = math.Round(float64(metaFetched)/float64(s.TokensIndexed)*10000) / 100
		}
		items = append(items, s)
	}

	RespondJSON(w, http.StatusOK, APIResponse{Data: items})
}
type QueueStatus struct {
	DiscoveryQueueSize int            `json:"discovery_queue_size"`
	PendingDiscovery   []string       `json:"pending_discovery"`
	MetadataQueues     map[string]int `json:"metadata_queues"`
}

// GET /v1/queue
// Returns the current state of discovery and metadata queues by querying the database.
func (a *API) HandleQueueStatus(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	status := QueueStatus{
		MetadataQueues:   make(map[string]int),
		PendingDiscovery: []string{},
	}

	// 1. Get collections currently snapshotting or reindexing
	rows, err := a.Store.Pool.Query(ctx, `
		SELECT address FROM collections 
		WHERE snapshot_done = false OR reindexing = true
	`)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var addr string
			if err := rows.Scan(&addr); err == nil {
				status.PendingDiscovery = append(status.PendingDiscovery, addr)
			}
		}
		status.DiscoveryQueueSize = len(status.PendingDiscovery)
	}

	// 2. Get metadata pending tokens count per contract
	// We only show collections that have > 0 pending to avoid clutter
	metaRows, err := a.Store.Pool.Query(ctx, `
		SELECT contract, COUNT(*) 
		FROM tokens 
		WHERE metadata_fetched = false 
		GROUP BY contract
	`)
	if err == nil {
		defer metaRows.Close()
		for metaRows.Next() {
			var contract string
			var count int
			if err := metaRows.Scan(&contract, &count); err == nil {
				status.MetadataQueues[contract] = count
			}
		}
	}

	RespondJSON(w, http.StatusOK, APIResponse{Data: status})
}
