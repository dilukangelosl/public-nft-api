package api

import (
	"math"
	"net/http"

	"github.com/go-chi/chi/v5"
	"go.uber.org/zap"
)

type CollectionStats struct {
	Address          string  `json:"address"`
	Name             string  `json:"name"`
	Symbol           string  `json:"symbol"`
	TotalSupply      int64   `json:"total_supply"`
	SnapshotDone     bool    `json:"snapshot_done"`
	SnapshotBlock    int64   `json:"snapshot_block"`
	TokensIndexed    int64   `json:"tokens_indexed"`
	BurnedCount      int64   `json:"burned_count"`
	UniqueOwners     int64   `json:"unique_owners"`
	MetadataFetched  int64   `json:"metadata_fetched"`
	MetadataPending  int64   `json:"metadata_pending"`
	MetadataProgress float64 `json:"metadata_progress_pct"`
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
