package indexer

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/dilukangelosl/public-nft-api/internal/chain"
	"github.com/dilukangelosl/public-nft-api/internal/store"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
	"golang.org/x/time/rate"
)

const (
	BatchSize            = 500
	MaxConcurrentBatches = 3   // max parallel Multicall3 batches per snapshot
)

// globalRPCLimiter caps total RPC calls across all concurrent snapshots
var globalRPCLimiter = rate.NewLimiter(rate.Limit(5), 5) // 5 RPC calls/sec burst 5

func ProcessSnapshot(
	ctx context.Context,
	ethClient *chain.Client,
	dbStore *store.Store,
	contract string,
	snapshotBlock uint64,
	logger *zap.Logger,
	metadataQueue map[string]chan string,
	metadataMutex *sync.RWMutex,
) error {
	hasMetadataQueue := metadataQueue != nil && metadataMutex != nil
	logger.Info("Starting snapshot for collection", zap.String("contract", contract))

	// 1. Fetch Collection Metadata: name, symbol, totalSupply
	nameCall, _ := chain.BuildCall(contract, "name")
	symCall, _ := chain.BuildCall(contract, "symbol")
	tsCall, _ := chain.BuildCall(contract, "totalSupply")

	metaRes, err := chain.Aggregate3(ctx, ethClient, []chain.Call3{nameCall, symCall, tsCall})
	if err != nil {
		return fmt.Errorf("metadata fetch Multicall3 failed: %w", err)
	}

	name, _ := chain.DecodeString("name", metaRes[0].ReturnData)
	symbol, _ := chain.DecodeString("symbol", metaRes[1].ReturnData)
	tsUint, err := chain.DecodeUint256("totalSupply", metaRes[2].ReturnData)
	if err != nil {
		return fmt.Errorf("failed to fetch valid totalSupply for %s: %w", contract, err)
	}

	totalSupply := tsUint.Int64()
	if totalSupply <= 0 {
		return fmt.Errorf("invalid total supply 0 for %s", contract)
	}

	// 2. Detect starting index
	startIndex, _, err := DetectStartIndex(ctx, ethClient, contract, logger)
	if err != nil {
		logger.Error("Failed start index detection. Defaulting to 1.", zap.Error(err))
		startIndex = 1
	}

	// Persist Collection Metadata to DB initially as non-complete snapshot
	coll := store.Collection{
		Address:       contract,
		Name:          name,
		Symbol:        symbol,
		TotalSupply:   totalSupply,
		StartIndex:    startIndex,
		SnapshotDone:  false,
		SnapshotBlock: int64(snapshotBlock),
	}
	if err := dbStore.CreateCollection(ctx, coll); err != nil {
		return fmt.Errorf("failed to create collection object: %w", err)
	}

	// Prepare metadata queue channel for this contract
	if hasMetadataQueue {
		channelSize := totalSupply
		if channelSize > 50000 {
			channelSize = 50000
		}
		metadataMutex.Lock()
		if _, exists := metadataQueue[contract]; !exists {
			metadataQueue[contract] = make(chan string, channelSize)
		}
		metadataMutex.Unlock()
	}

	// 3. Multicall3 batch fetching of Owners
	// Max cap total supply snapshot processing locally (for example some arbitrary logic collections can return MAX_INT)
	if totalSupply > 1_000_000 {
		logger.Warn("Total Supply is exceptionally high, capping local snapshot limit strictly", zap.Int64("supply", totalSupply))
		totalSupply = 1_000_000
	}

	sem := semaphore.NewWeighted(int64(MaxConcurrentBatches))
	var wg sync.WaitGroup

	// Gather tokens into chunks
	var chunks [][]int64
	var currentChunk []int64
	endIndex := int64(startIndex) + totalSupply

	for i := int64(startIndex); i < endIndex; i++ {
		currentChunk = append(currentChunk, i)
		if len(currentChunk) == BatchSize {
			chunks = append(chunks, currentChunk)
			currentChunk = nil
		}
	}
	if len(currentChunk) > 0 {
		chunks = append(chunks, currentChunk)
	}

	tokenResults := make(chan store.Token, totalSupply)
	errChan := make(chan error, len(chunks))

	for _, chunk := range chunks {
		wg.Add(1)

		// Wait for global rate limiter before acquiring semaphore slot
		if waitErr := globalRPCLimiter.Wait(ctx); waitErr != nil {
			errChan <- waitErr
			wg.Done()
			continue
		}

		err := sem.Acquire(ctx, 1)
		if err != nil {
			errChan <- err
			wg.Done()
			continue
		}

		go func(tokenIDs []int64) {
			defer sem.Release(1)
			defer wg.Done()

			var batchCalls []chain.Call3
			for _, tid := range tokenIDs {
				call, _ := chain.BuildCall(contract, "ownerOf", big.NewInt(tid))
				batchCalls = append(batchCalls, call)
			}

			batchRes, err := chain.Aggregate3(ctx, ethClient, batchCalls)
			if err != nil {
				logger.Error("Chunk ownerOf multicall failed, some tokens skipped", zap.Error(err))
				return
			}

			for i, r := range batchRes {
				if r.Success {
					ownerAddr, decErr := chain.DecodeAddress(r.ReturnData)
					if decErr == nil {
						tokenIDStr := big.NewInt(tokenIDs[i]).String()
						tokenResults <- store.Token{
							Contract:        contract,
							TokenID:         tokenIDStr,
							Owner:           ownerAddr,
							MetadataFetched: false,
							UpdatedAt:       time.Now().UTC(),
						}

						if hasMetadataQueue {
							select {
							case metadataQueue[contract] <- tokenIDStr:
							default:
								logger.Warn("Metadata queue full, dropping immediate enqueue", zap.String("tokenId", tokenIDStr))
							}
						}
					}
				}
			}
		}(chunk)
	}

	// Wait in goroutine to close channel naturally
	go func() {
		wg.Wait()
		close(tokenResults)
		close(errChan)
	}()

	var allTokens []store.Token
	for t := range tokenResults {
		allTokens = append(allTokens, t)
	}

	if len(allTokens) > 0 {
		if err := dbStore.BulkUpsertTokens(ctx, allTokens); err != nil {
			return fmt.Errorf("bulk DB upsert failed: %w", err)
		}
	}

	// 4. Mark collection snapshot done
	if err := dbStore.UpdateCollectionSnapshotDone(ctx, contract); err != nil {
		return fmt.Errorf("failed to mark snapshot_done: %w", err)
	}

	logger.Info("Snapshot complete", zap.String("contract", contract), zap.Int("tokens", len(allTokens)))
	return nil
}
