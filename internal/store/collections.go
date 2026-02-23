package store

import (
	"context"
	"fmt"
	"time"
)

type Collection struct {
	Address       string    `json:"address"`
	Name          string    `json:"name"`
	Symbol        string    `json:"symbol"`
	Type          string    `json:"type"`
	TotalSupply   int64     `json:"total_supply"`
	StartIndex    int       `json:"start_index"`
	SnapshotDone  bool      `json:"snapshot_done"`
	SnapshotBlock int64     `json:"snapshot_block"`
	Reindexing    bool      `json:"reindexing"`
	CreatedAt     time.Time `json:"created_at"`
	BurnedCount   int64     `json:"burned_count,omitempty"` // computed
}

func (s *Store) CreateCollection(ctx context.Context, coll Collection) error {
	query := `
		INSERT INTO collections (address, name, symbol, total_supply, start_index, snapshot_block)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (address) DO NOTHING
	`
	_, err := s.Pool.Exec(ctx, query,
		coll.Address, coll.Name, coll.Symbol, coll.TotalSupply, coll.StartIndex, coll.SnapshotBlock)
	if err != nil {
		return fmt.Errorf("error creating collection %s: %w", coll.Address, err)
	}
	return nil
}

func (s *Store) UpdateCollectionSnapshotDone(ctx context.Context, address string) error {
	query := `UPDATE collections SET snapshot_done = true WHERE address = $1`
	_, err := s.Pool.Exec(ctx, query, address)
	if err != nil {
		return fmt.Errorf("error updating snapshot complete for %s: %w", address, err)
	}
	return nil
}

func (s *Store) GetCollection(ctx context.Context, address string) (*Collection, error) {
	query := `
		SELECT 
			c.address, c.name, c.symbol, c.type, c.total_supply, c.start_index, 
			c.snapshot_done, c.snapshot_block, c.reindexing, c.created_at,
			(SELECT count(*) FROM tokens t WHERE t.contract = c.address AND t.owner IN ('0x0000000000000000000000000000000000000000', '0x000000000000000000000000000000000000dEaD')) as burned_count
		FROM collections c 
		WHERE c.address = $1
	`
	var c Collection
	err := s.Pool.QueryRow(ctx, query, address).Scan(
		&c.Address, &c.Name, &c.Symbol, &c.Type, &c.TotalSupply, &c.StartIndex,
		&c.SnapshotDone, &c.SnapshotBlock, &c.Reindexing, &c.CreatedAt, &c.BurnedCount)

	if err != nil {
		return nil, fmt.Errorf("error getting collection %s: %w", address, err)
	}
	return &c, nil
}

func (s *Store) SetCollectionReindexing(ctx context.Context, address string, state bool) error {
	query := `UPDATE collections SET reindexing = $1 WHERE address = $2`
	_, err := s.Pool.Exec(ctx, query, state, address)
	if err != nil {
		return fmt.Errorf("error setting reindexing %v for %s: %w", state, address, err)
	}
	return nil
}
