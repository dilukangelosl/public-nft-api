package store

import (
	"context"
	"fmt"
)

type Metadata struct {
	Contract    string      `json:"contract"`
	TokenID     string      `json:"token_id"`
	Name        string      `json:"name"`
	Description string      `json:"description"`
	Image       string      `json:"image"`
	Attributes  interface{} `json:"attributes"` // maps directly to JSONB
}

func (s *Store) UpsertMetadata(ctx context.Context, m Metadata) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("error beginning metadata tx: %w", err)
	}
	defer tx.Rollback(ctx)

	query := `
		INSERT INTO metadata (contract, token_id, name, description, image, attributes)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (contract, token_id) DO UPDATE SET 
			name = EXCLUDED.name,
			description = EXCLUDED.description,
			image = EXCLUDED.image,
			attributes = EXCLUDED.attributes
	`
	_, err = tx.Exec(ctx, query, m.Contract, m.TokenID, m.Name, m.Description, m.Image, m.Attributes)
	if err != nil {
		return fmt.Errorf("error upserting metadata for %s-%s: %w", m.Contract, m.TokenID, err)
	}

	// Mark metadata as fetched in tokens table
	markQuery := `UPDATE tokens SET metadata_fetched = true WHERE contract = $1 AND token_id = $2`
	_, err = tx.Exec(ctx, markQuery, m.Contract, m.TokenID)
	if err != nil {
		return fmt.Errorf("error marking metadata as fetched: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("error committing metadata upsert tx: %w", err)
	}
	return nil
}

func (s *Store) ResetCollectionMetadataStatus(ctx context.Context, address string) error {
	query := `UPDATE tokens SET metadata_fetched = false WHERE contract = $1`
	_, err := s.Pool.Exec(ctx, query, address)
	if err != nil {
		return fmt.Errorf("error resetting metadata fetched flag for %s: %w", address, err)
	}
	return nil
}
