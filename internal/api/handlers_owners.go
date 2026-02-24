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
	withMetadata := r.URL.Query().Get("metadata") == "true"

	var total int
	if err := a.Store.Pool.QueryRow(r.Context(), "SELECT count(*) FROM owner_tokens WHERE owner = $1", addressStr).Scan(&total); err != nil {
		RespondError(w, http.StatusInternalServerError, "db_error", "Failed to count owner tokens")
		return
	}

	type OwnerTokenItem struct {
		Contract       string  `json:"contract"`
		CollectionName *string `json:"collection_name,omitempty"`
		CollectionSym  *string `json:"collection_symbol,omitempty"`
		TokenID        string  `json:"token_id"`
		Name           *string `json:"name,omitempty"`
		Image          *string `json:"image,omitempty"`
	}

	var items []OwnerTokenItem

	if withMetadata {
		query := `
			SELECT ot.contract, ot.token_id, c.name, c.symbol, m.name, m.image
			FROM owner_tokens ot
			JOIN collections c ON c.address = ot.contract
			LEFT JOIN metadata m ON m.contract = ot.contract AND m.token_id = ot.token_id
			WHERE ot.owner = $1
			ORDER BY ot.contract ASC, CASE WHEN ot.token_id ~ '^[0-9]+$' THEN ot.token_id::NUMERIC ELSE NULL END ASC, ot.token_id ASC
			LIMIT $2 OFFSET $3
		`
		rows, err := a.Store.Pool.Query(r.Context(), query, addressStr, limit, offset)
		if err != nil {
			RespondError(w, http.StatusInternalServerError, "db_error", "Failed to query owner tokens")
			return
		}
		defer rows.Close()
		for rows.Next() {
			var i OwnerTokenItem
			if err := rows.Scan(&i.Contract, &i.TokenID, &i.CollectionName, &i.CollectionSym, &i.Name, &i.Image); err == nil {
				items = append(items, i)
			} else {
				a.Logger.Error("failed to scan owner token item", zap.Error(err))
			}
		}
	} else {
		query := `
			SELECT ot.contract, ot.token_id, c.name, c.symbol
			FROM owner_tokens ot
			JOIN collections c ON c.address = ot.contract
			WHERE ot.owner = $1
			ORDER BY ot.contract ASC, CASE WHEN ot.token_id ~ '^[0-9]+$' THEN ot.token_id::NUMERIC ELSE NULL END ASC, ot.token_id ASC
			LIMIT $2 OFFSET $3
		`
		rows, err := a.Store.Pool.Query(r.Context(), query, addressStr, limit, offset)
		if err != nil {
			RespondError(w, http.StatusInternalServerError, "db_error", "Failed to query owner tokens")
			return
		}
		defer rows.Close()
		for rows.Next() {
			var i OwnerTokenItem
			if err := rows.Scan(&i.Contract, &i.TokenID, &i.CollectionName, &i.CollectionSym); err == nil {
				items = append(items, i)
			}
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

// GET /v1/owners/:address/collections/:contract
// Returns all tokens owned by :address within :contract.
func (a *API) HandleGetOwnerCollectionTokens(w http.ResponseWriter, r *http.Request) {
	address := strings.ToLower(chi.URLParam(r, "address"))
	contract := chi.URLParam(r, "contract")
	page, limit := GetPagination(r)
	offset := (page - 1) * limit
	withMetadata := r.URL.Query().Get("metadata") == "true"

	var total int
	if err := a.Store.Pool.QueryRow(r.Context(),
		`SELECT count(*) FROM owner_tokens WHERE owner = $1 AND contract = $2`,
		address, contract,
	).Scan(&total); err != nil {
		RespondError(w, http.StatusInternalServerError, "db_error", "Failed to count tokens")
		return
	}

	type Item struct {
		TokenID     string      `json:"token_id"`
		Owner       string      `json:"owner"`
		Name        *string     `json:"name,omitempty"`
		Description *string     `json:"description,omitempty"`
		Image       *string     `json:"image,omitempty"`
		Attributes  interface{} `json:"attributes,omitempty"`
	}

	var items []Item

	if withMetadata {
		rows, err := a.Store.Pool.Query(r.Context(), `
			SELECT t.token_id, t.owner, m.name, m.description, m.image, m.attributes
			FROM tokens t
			LEFT JOIN metadata m ON m.contract = t.contract AND m.token_id = t.token_id
			WHERE t.contract = $1 AND t.owner = $2
			ORDER BY CASE WHEN t.token_id ~ '^[0-9]+$' THEN t.token_id::NUMERIC ELSE NULL END ASC, t.token_id ASC
			LIMIT $3 OFFSET $4
		`, contract, address, limit, offset)
		if err != nil {
			RespondError(w, http.StatusInternalServerError, "db_error", "Failed to query tokens")
			return
		}
		defer rows.Close()
		for rows.Next() {
			var i Item
			if err := rows.Scan(&i.TokenID, &i.Owner, &i.Name, &i.Description, &i.Image, &i.Attributes); err == nil {
				items = append(items, i)
			} else {
				a.Logger.Error("failed to scan owner collection token", zap.Error(err))
			}
		}
	} else {
		rows, err := a.Store.Pool.Query(r.Context(), `
			SELECT token_id, owner
			FROM tokens
			WHERE contract = $1 AND owner = $2
			ORDER BY CASE WHEN token_id ~ '^[0-9]+$' THEN token_id::NUMERIC ELSE NULL END ASC, token_id ASC
			LIMIT $3 OFFSET $4
		`, contract, address, limit, offset)
		if err != nil {
			RespondError(w, http.StatusInternalServerError, "db_error", "Failed to query tokens")
			return
		}
		defer rows.Close()
		for rows.Next() {
			var i Item
			if err := rows.Scan(&i.TokenID, &i.Owner); err == nil {
				items = append(items, i)
			}
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
