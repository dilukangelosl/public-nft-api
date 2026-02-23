package api

import (
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"go.uber.org/zap"
)

// GET /v1/collections/:contract/owners
func (a *API) HandleGetCollectionOwners(w http.ResponseWriter, r *http.Request) {
	contract := chi.URLParam(r, "contract")
	page, limit := GetPagination(r)
	includeBurned := r.URL.Query().Get("include_burned") == "true"
	offset := (page - 1) * limit

	burnFilter := "AND t.owner NOT IN ('0x0000000000000000000000000000000000000000', '0x000000000000000000000000000000000000dEaD')"
	if includeBurned {
		burnFilter = ""
	}

	// Count total unique owners
	var total int
	countQuery := "SELECT count(DISTINCT owner) FROM tokens t WHERE t.contract = $1 " + burnFilter
	err := a.Store.Pool.QueryRow(r.Context(), countQuery, contract).Scan(&total)
	if err != nil {
		RespondError(w, http.StatusInternalServerError, "db_error", "Failed to count owners")
		return
	}

	// Group tokens by owner
	query := `
		SELECT owner, count(*) as token_count
		FROM tokens t
		WHERE t.contract = $1 ` + burnFilter + `
		GROUP BY owner
		ORDER BY token_count DESC, owner ASC
		LIMIT $2 OFFSET $3
	`

	rows, err := a.Store.Pool.Query(r.Context(), query, contract, limit, offset)
	if err != nil {
		RespondError(w, http.StatusInternalServerError, "db_error", "Failed to query owners")
		return
	}
	defer rows.Close()

	type OwnerStat struct {
		Owner      string `json:"owner"`
		TokenCount int    `json:"token_count"`
	}

	var items []OwnerStat
	for rows.Next() {
		var i OwnerStat
		if err := rows.Scan(&i.Owner, &i.TokenCount); err == nil {
			items = append(items, i)
		} else {
			a.Logger.Error("failed to scan owner stat", zap.Error(err))
		}
	}

	RespondJSON(w, http.StatusOK, APIResponse{
		Data: items,
		Pagination: &Pagination{
			Page:    page,
			Limit:   limit,
			Total:   total,
			HasNext: (offset + limit) < total,
		},
	})
}

// GET /v1/owners/:address
func (a *API) HandleGetOwnerTokens(w http.ResponseWriter, r *http.Request) {
	address := chi.URLParam(r, "address")
	addressStr := strings.ToLower(address)
	
	page, limit := GetPagination(r)
	offset := (page - 1) * limit

	// Count total
	var total int
	countQuery := "SELECT count(*) FROM owner_tokens WHERE owner = $1"
	err := a.Store.Pool.QueryRow(r.Context(), countQuery, addressStr).Scan(&total)
	if err != nil {
		RespondError(w, http.StatusInternalServerError, "db_error", "Failed to count owner tokens")
		return
	}

	// Fetch join
	query := `
		SELECT ot.contract, ot.token_id, c.name, c.symbol, m.name, m.image
		FROM owner_tokens ot
		JOIN collections c ON c.address = ot.contract
		LEFT JOIN metadata m ON m.contract = ot.contract AND m.token_id = ot.token_id
		WHERE ot.owner = $1
		ORDER BY ot.contract ASC, CAST(ot.token_id AS NUMERIC) ASC
		LIMIT $2 OFFSET $3
	`

	rows, err := a.Store.Pool.Query(r.Context(), query, addressStr, limit, offset)
	if err != nil {
		RespondError(w, http.StatusInternalServerError, "db_error", "Failed to query owner tokens")
		return
	}
	defer rows.Close()

	type OwnerTokenItem struct {
		Contract        string  `json:"contract"`
		CollectionName  *string `json:"collection_name,omitempty"`
		CollectionSym   *string `json:"collection_symbol,omitempty"`
		TokenID         string  `json:"token_id"`
		MetadataName    *string `json:"name,omitempty"`
		MetadataImage   *string `json:"image,omitempty"`
	}

	var items []OwnerTokenItem
	for rows.Next() {
		var i OwnerTokenItem
		if err := rows.Scan(&i.Contract, &i.TokenID, &i.CollectionName, &i.CollectionSym, &i.MetadataName, &i.MetadataImage); err == nil {
			items = append(items, i)
		} else {
			a.Logger.Error("failed to scan owner token item", zap.Error(err))
		}
	}

	RespondJSON(w, http.StatusOK, APIResponse{
		Data: items,
		Pagination: &Pagination{
			Page:    page,
			Limit:   limit,
			Total:   total,
			HasNext: (offset + limit) < total,
		},
	})
}
