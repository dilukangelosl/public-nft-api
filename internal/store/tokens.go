package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

type Token struct {
	Contract        string    `json:"contract"`
	TokenID         string    `json:"token_id"`
	Owner           string    `json:"owner"`
	MetadataFetched bool      `json:"metadata_fetched"`
	UpdatedAt       time.Time `json:"updated_at"`
}

// BulkInsertTokens processes a large batch of tokens using pgx CopyFrom,
// and simultaneously populates both the `tokens` and `owner_tokens` tables.
func (s *Store) BulkInsertTokens(ctx context.Context, tokens []Token) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("error beginning tx for bulk tokens insert: %w", err)
	}
	defer tx.Rollback(ctx)

	var tokenRows [][]interface{}
	var ownerRows [][]interface{}
	for _, t := range tokens {
		tokenRows = append(tokenRows, []interface{}{t.Contract, t.TokenID, t.Owner, t.MetadataFetched, t.UpdatedAt})
		ownerRows = append(ownerRows, []interface{}{t.Owner, t.Contract, t.TokenID})
	}

	// 1. Insert into tokens table
	_, err = tx.CopyFrom(
		ctx,
		pgx.Identifier{"tokens"},
		[]string{"contract", "token_id", "owner", "metadata_fetched", "updated_at"},
		pgx.CopyFromRows(tokenRows),
	)
	if err != nil {
		return fmt.Errorf("error inserting bulk tokens: %w", err)
	}

	// 2. Insert into owner_tokens reverse index
	_, err = tx.CopyFrom(
		ctx,
		pgx.Identifier{"owner_tokens"},
		[]string{"owner", "contract", "token_id"},
		pgx.CopyFromRows(ownerRows),
	)
	if err != nil {
		return fmt.Errorf("error inserting bulk owner_tokens: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("error committing bulk tokens insert tx: %w", err)
	}

	return nil
}

// UpdateOwnership handles single Transfer events update logic using a transaction
func (s *Store) UpdateOwnership(ctx context.Context, contract, to, tokenId string) error {
	// First fetch the old owner to delete from reverse index
	var oldOwner string
	err := s.Pool.QueryRow(ctx, `SELECT owner FROM tokens WHERE contract = $1 AND token_id = $2`, contract, tokenId).Scan(&oldOwner)
	if err != nil && err != pgx.ErrNoRows {
		return fmt.Errorf("error fetching old owner: %w", err)
	}

	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("error beginning update tx: %w", err)
	}
	defer tx.Rollback(ctx)

	// Update tokens table
	_, err = tx.Exec(ctx, `
		INSERT INTO tokens (contract, token_id, owner, updated_at)
		VALUES ($1, $2, $3, NOW())
		ON CONFLICT (contract, token_id) DO UPDATE SET owner = EXCLUDED.owner, updated_at = NOW()
	`, contract, tokenId, to)
	if err != nil {
		return fmt.Errorf("error upserting to tokens: %w", err)
	}

	// Handle reverse index
	if oldOwner != "" {
		_, err = tx.Exec(ctx, `DELETE FROM owner_tokens WHERE owner = $1 AND contract = $2 AND token_id = $3`, oldOwner, contract, tokenId)
		if err != nil {
			return fmt.Errorf("error deleting old reverse index: %w", err)
		}
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO owner_tokens (owner, contract, token_id)
		VALUES ($1, $2, $3)
		ON CONFLICT (owner, contract, token_id) DO NOTHING
	`, to, contract, tokenId)
	if err != nil {
		return fmt.Errorf("error inserting new reverse index: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("error committing token update: %w", err)
	}
	return nil
}
