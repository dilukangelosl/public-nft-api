package cache

import (
	"fmt"
	"time"

	"github.com/dgraph-io/ristretto"
	"go.uber.org/zap"
)

type Cache struct {
	ristretto *ristretto.Cache
	logger    *zap.Logger
}

func NewCache(logger *zap.Logger) (*Cache, error) {
	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 1e7,     // number of keys to track frequency of (10M).
		MaxCost:     1 << 28, // maximum cost of cache (256MB).
		BufferItems: 64,      // number of keys per Get buffer.
		Metrics:     true,    // track cache hit/miss ratio for API /health diagnostics.
	})
	if err != nil {
		return nil, fmt.Errorf("failed creating ristretto cache instance: %w", err)
	}

	return &Cache{
		ristretto: cache,
		logger:    logger,
	}, nil
}

func (c *Cache) Get(key string) ([]byte, bool) {
	val, found := c.ristretto.Get(key)
	if !found {
		return nil, false
	}
	bytes, ok := val.([]byte)
	if !ok {
		return nil, false
	}
	return bytes, true
}

func (c *Cache) Set(key string, value []byte, ttl time.Duration) bool {
	// The cost is roughly the byte array sizing in memory.
	cost := int64(len(value))
	return c.ristretto.SetWithTTL(key, value, cost, ttl)
}

func (c *Cache) Invalidate(key string) {
	c.ristretto.Del(key)
}

func (c *Cache) Wait() {
	// Wait allows async processing to finish inside ristretto
	c.ristretto.Wait()
}
