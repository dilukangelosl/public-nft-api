package indexer

import (
	"context"
	"sync"
	"time"

	"github.com/dilukangelosl/public-nft-api/internal/chain"
	"github.com/dilukangelosl/public-nft-api/internal/store"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"go.uber.org/zap"
)

type Listener struct {
	ethClient     *chain.Client
	dbStore       *store.Store
	logger        *zap.Logger
	metadataQueue map[string]chan string
	metadataMutex *sync.RWMutex

	// sync.Maps for state tracking
	knownContracts   sync.Map
	pendingContracts sync.Map
	blacklisted      sync.Map

	discoveryChan chan string
}

func NewListener(eth *chain.Client, db *store.Store, log *zap.Logger, mdQ map[string]chan string, mdMutex *sync.RWMutex) *Listener {
	return &Listener{
		ethClient:     eth,
		dbStore:       db,
		logger:        log,
		metadataQueue: mdQ,
		metadataMutex: mdMutex,
		discoveryChan: make(chan string, 1000), // Buffered Discovery Channel
	}
}

func (l *Listener) Start(ctx context.Context) {
	go l.runDiscoveryLoop(ctx)
	go l.runManualDiscoveryListener(ctx)
	l.runWebSocketLoop(ctx)
}

func (l *Listener) runManualDiscoveryListener(ctx context.Context) {
	conn, err := l.dbStore.Pool.Acquire(ctx)
	if err != nil {
		l.logger.Error("Failed to acquire connection for Postgres LISTEN", zap.Error(err))
		return
	}
	defer conn.Release()

	_, err = conn.Exec(ctx, "LISTEN manual_discovery")
	if err != nil {
		l.logger.Error("Failed to LISTEN for manual_discovery", zap.Error(err))
		return
	}

	for {
		notification, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			l.logger.Error("Error waiting for notification", zap.Error(err))
			continue
		}

		contract := notification.Payload
		if _, known := l.knownContracts.Load(contract); !known {
			if _, pending := l.pendingContracts.LoadOrStore(contract, true); !pending {
				l.discoveryChan <- contract
				l.logger.Info("Received Postgres NOTIFY for manual indexing", zap.String("contract", contract))
			}
		}
	}
}

func (l *Listener) runWebSocketLoop(ctx context.Context) {
	query := ethereum.FilterQuery{
		Topics: [][]common.Hash{{chain.TransferEventHash}}, // Listen to ALL Transfer events
	}

	for {
		logs := make(chan types.Log)
		sub, err := l.ethClient.WSS.SubscribeFilterLogs(ctx, query, logs)
		if err != nil {
			l.logger.Error("Failed subscribing to logs. Retrying in 5 seconds...", zap.Error(err))
			select {
			case <-time.After(5 * time.Second):
				continue
			case <-ctx.Done():
				return
			}
		}

		l.logger.Info("Successfully subscribed to Ethereum Transfer active WebSocket logs")

		for {
			select {
			case err := <-sub.Err():
				l.logger.Warn("WebSocket subscription error. Reconnecting...", zap.Error(err))
				time.Sleep(2 * time.Second) // basic backoff
				goto RECONNECT
			case vLog := <-logs:
				// ERC-721 Transfer events have EXACTLY 4 topics (Signature, From, To, TokenId)
				// ERC-20 Transfer events have 3 topics
				if len(vLog.Topics) != 4 {
					continue
				}

				contractAddr := vLog.Address.Hex()
				from := common.HexToAddress(vLog.Topics[1].Hex()).Hex()
				to := common.HexToAddress(vLog.Topics[2].Hex()).Hex()
				tokenID := vLog.Topics[3].Big().String()
				blockNum := vLog.BlockNumber

				l.handleTransferLog(ctx, contractAddr, from, to, tokenID, blockNum)
			case <-ctx.Done():
				return
			}
		}

	RECONNECT:
	}
}

func (l *Listener) handleTransferLog(ctx context.Context, contract, from, to, tokenID string, blockNum uint64) {
	// 1. Is it a known blacklisted contract? Discard instantly.
	if _, bad := l.blacklisted.Load(contract); bad {
		return
	}

	// 2. Is it a fully known contract we track?
	if _, known := l.knownContracts.Load(contract); known {
		// Verify if the block is before its snapshot started
		// Note: to be fully safe with block overlaps, production systems usually check the snapshot_block threshold
		// We process it directly.
		err := l.dbStore.UpdateOwnership(ctx, contract, to, tokenID)
		if err != nil {
			l.logger.Error("Failed to update ownership for known transfer", zap.String("contract", contract), zap.String("token", tokenID), zap.Error(err))
		}
		
		// If the token ID isn't in DB, the 'update ownership' might result in no rows affected, or create it empty.
		// Re-enqueuing metadata fetch for new mints dynamically here might be necessary
		l.metadataMutex.RLock()
		if ch, ok := l.metadataQueue[contract]; ok {
			select {
			case ch <- tokenID:
			default:
			}
		}
		l.metadataMutex.RUnlock()
		return
	}

	// 3. New contract. Check if it's already pending snapshot discovery
	if _, pending := l.pendingContracts.LoadOrStore(contract, true); !pending {
		l.logger.Info("New unindexed contract transfer detected, pushing to Discovery Channel", zap.String("contract", contract))
		select {
		case l.discoveryChan <- contract:
		default:
			l.logger.Warn("Discovery channel full, dropped prospective contract", zap.String("contract", contract))
			l.pendingContracts.Delete(contract) // allow it to be picked up again later
		}
	}
}

// runDiscoveryLoop acts as the worker pool taking contracts from Discovery
func (l *Listener) runDiscoveryLoop(ctx context.Context) {
	for {
		select {
		case contract := <-l.discoveryChan:
			// Detector: Validate ERC-721 Interface
			isERC721, err := ValidateERC721(ctx, l.ethClient, contract)
			if err != nil {
				l.logger.Warn("Failed to validate ERC-721 interface. Re-queueing logic required or dropping.", zap.String("contract", contract), zap.Error(err))
				l.pendingContracts.Delete(contract)
				continue
			}

			if !isERC721 {
				l.blacklisted.Store(contract, true)
				l.pendingContracts.Delete(contract)
				continue
			}

			// Valid ERC-721! Run Snapshot Indexer
			block, err := l.ethClient.HTTP.BlockNumber(ctx)
			if err != nil {
				l.logger.Error("Failed to get latest block for snapshot check", zap.Error(err))
				l.pendingContracts.Delete(contract)
				continue
			}

			err = ProcessSnapshot(ctx, l.ethClient, l.dbStore, contract, block, l.logger, l.metadataQueue, l.metadataMutex)
			if err != nil {
				l.logger.Error("Snapshot processing failed", zap.String("contract", contract), zap.Error(err))
				l.pendingContracts.Delete(contract)
				continue
			}

			// Successfully Snapshotted. Promote from Pending -> Known
			l.knownContracts.Store(contract, true)
			l.pendingContracts.Delete(contract)

		case <-ctx.Done():
			return
		}
	}
}

// ManuallyEnqueue is exposed for the API server POST /v1/collections Endpoint
func (l *Listener) ManuallyEnqueue(contract string) bool {
	if _, known := l.knownContracts.Load(contract); known {
		return false // Already indexed
	}
	if _, pending := l.pendingContracts.LoadOrStore(contract, true); !pending {
		select {
		case l.discoveryChan <- contract:
			return true
		default:
			l.pendingContracts.Delete(contract)
			return false
		}
	}
	return true // Already pending
}
