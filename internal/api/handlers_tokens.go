package api

import (
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"go.uber.org/zap"
)

// GET /v1/collections/:contract/tokens
func (a *API) HandleListTokens(w http.ResponseWriter, r *http.Request) {
	contract := chi.URLParam(r, "contract")
	page, limit := GetPagination(r)
	includeBurned := r.URL.Query().Get("include_burned") == "true"
	withMetadata := r.URL.Query().Get("metadata") == "true"
	offset := (page - 1) * limit

	burnFilter := "AND t.owner NOT IN ('0x0000000000000000000000000000000000000000', '0x000000000000000000000000000000000000dEaD')"
	if includeBurned {
		burnFilter = ""
	}

	var total int
	countQuery := "SELECT count(*) FROM tokens t WHERE t.contract = $1 " + burnFilter
	if err := a.Store.Pool.QueryRow(r.Context(), countQuery, contract).Scan(&total); err != nil {
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
		query := `
			SELECT t.token_id, t.owner, m.name, m.description, m.image, m.attributes
			FROM tokens t
			LEFT JOIN metadata m ON t.contract = m.contract AND t.token_id = m.token_id
			WHERE t.contract = $1 ` + burnFilter + `
			ORDER BY CASE WHEN t.token_id ~ '^[0-9]+$' THEN t.token_id::NUMERIC ELSE NULL END ASC, t.token_id ASC
			LIMIT $2 OFFSET $3
		`
		rows, err := a.Store.Pool.Query(r.Context(), query, contract, limit, offset)
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
				a.Logger.Error("failed to scan item", zap.Error(err))
			}
		}
	} else {
		query := `
			SELECT token_id, owner
			FROM tokens
			WHERE contract = $1 ` + burnFilter + `
			ORDER BY CASE WHEN token_id ~ '^[0-9]+$' THEN token_id::NUMERIC ELSE NULL END ASC, token_id ASC
			LIMIT $2 OFFSET $3
		`
		rows, err := a.Store.Pool.Query(r.Context(), query, contract, limit, offset)
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

// GET /v1/collections/:contract/tokens/:tokenId
func (a *API) HandleGetToken(w http.ResponseWriter, r *http.Request) {
	contract := chi.URLParam(r, "contract")
	tokenID := chi.URLParam(r, "tokenId")
	withMetadata := r.URL.Query().Get("metadata") == "true"

	type TokenResponse struct {
		Contract    string      `json:"contract"`
		TokenID     string      `json:"token_id"`
		Owner       string      `json:"owner"`
		Name        *string     `json:"name,omitempty"`
		Description *string     `json:"description,omitempty"`
		Image       *string     `json:"image,omitempty"`
		Attributes  interface{} `json:"attributes,omitempty"`
	}

	if withMetadata {
		query := `
			SELECT t.owner, m.name, m.description, m.image, m.attributes
			FROM tokens t
			LEFT JOIN metadata m ON t.contract = m.contract AND t.token_id = m.token_id
			WHERE t.contract = $1 AND t.token_id = $2
		`
		var owner string
		var name, desc, img *string
		var attrs interface{}
		if err := a.Store.Pool.QueryRow(r.Context(), query, contract, tokenID).Scan(&owner, &name, &desc, &img, &attrs); err != nil {
			if strings.Contains(err.Error(), "no rows") {
				RespondError(w, http.StatusNotFound, "not_found", "Token not found")
				return
			}
			RespondError(w, http.StatusInternalServerError, "db_error", "Failed to query token")
			return
		}
		RespondJSON(w, http.StatusOK, APIResponse{Data: TokenResponse{
			Contract: contract, TokenID: tokenID, Owner: owner,
			Name: name, Description: desc, Image: img, Attributes: attrs,
		}})
	} else {
		var owner string
		if err := a.Store.Pool.QueryRow(r.Context(), `SELECT owner FROM tokens WHERE contract = $1 AND token_id = $2`, contract, tokenID).Scan(&owner); err != nil {
			if strings.Contains(err.Error(), "no rows") {
				RespondError(w, http.StatusNotFound, "not_found", "Token not found")
				return
			}
			RespondError(w, http.StatusInternalServerError, "db_error", "Failed to query token")
			return
		}
		RespondJSON(w, http.StatusOK, APIResponse{Data: TokenResponse{
			Contract: contract, TokenID: tokenID, Owner: owner,
		}})
	}
}
