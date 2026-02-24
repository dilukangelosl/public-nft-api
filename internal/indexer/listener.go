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

type pendingTransfer struct {
	to      string
	tokenID string
}

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

	// Buffers Transfer events that arrive while a snapshot is in-flight.
	// Keyed by contract address, drained after snapshot completes.
	pendingTransfers   map[string][]pendingTransfer
	pendingTransfersMu sync.Mutex

	discoveryChan chan string
}

func NewListener(eth *chain.Client, db *store.Store, log *zap.Logger, mdQ map[string]chan string, mdMutex *sync.RWMutex) *Listener {
	return &Listener{
		ethClient:        eth,
		dbStore:          db,
		logger:           log,
		metadataQueue:    mdQ,
		metadataMutex:    mdMutex,
		pendingTransfers: make(map[string][]pendingTransfer),
		discoveryChan:    make(chan string, 1000),
	}
}

func (l *Listener) Start(ctx context.Context) {
	go l.runDiscoveryLoop(ctx)
	go l.runManualDiscoveryListener(ctx)
	l.runWebSocketLoop(ctx)
}

func (l *Listener) runManualDiscoveryListener(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}

		conn, err := l.dbStore.Pool.Acquire(ctx)
		if err != nil {
			l.logger.Error("Failed to acquire LISTEN connection, retrying in 5s", zap.Error(err))
			select {
			case <-time.After(5 * time.Second):
				continue
			case <-ctx.Done():
				return
			}
		}

		_, err = conn.Exec(ctx, "LISTEN manual_discovery")
		if err != nil {
			conn.Release()
			l.logger.Error("Failed to LISTEN for manual_discovery, retrying in 5s", zap.Error(err))
			select {
			case <-time.After(5 * time.Second):
				continue
			case <-ctx.Done():
				return
			}
		}

		l.logger.Info("Postgres LISTEN on manual_discovery channel active")

		for {
			notification, err := conn.Conn().WaitForNotification(ctx)
			if err != nil {
				conn.Release()
				if ctx.Err() != nil {
					return
				}
				l.logger.Warn("LISTEN connection dropped, reconnecting in 2s", zap.Error(err))
				time.Sleep(2 * time.Second)
				break // re-enter outer loop to re-acquire connection
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
	// 1. Blacklisted — discard instantly.
	if _, bad := l.blacklisted.Load(contract); bad {
		return
	}

	// 2. Fully known — apply immediately.
	if _, known := l.knownContracts.Load(contract); known {
		if err := l.dbStore.UpdateOwnership(ctx, contract, to, tokenID); err != nil {
			l.logger.Error("Failed to update ownership", zap.String("contract", contract), zap.String("token", tokenID), zap.Error(err))
		}
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

	// 3. Pending snapshot — buffer the transfer, apply after snapshot finishes.
	if _, pending := l.pendingContracts.Load(contract); pending {
		l.pendingTransfersMu.Lock()
		l.pendingTransfers[contract] = append(l.pendingTransfers[contract], pendingTransfer{to: to, tokenID: tokenID})
		l.pendingTransfersMu.Unlock()
		return
	}

	// 4. New contract — push to discovery.
	if _, alreadyPending := l.pendingContracts.LoadOrStore(contract, true); !alreadyPending {
		l.logger.Info("New unindexed contract detected, pushing to discovery", zap.String("contract", contract))
		select {
		case l.discoveryChan <- contract:
		default:
			l.logger.Warn("Discovery channel full, dropping contract", zap.String("contract", contract))
			l.pendingContracts.Delete(contract)
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

			// Promote to known BEFORE draining buffer so new transfers go direct path.
			l.knownContracts.Store(contract, true)
			l.pendingContracts.Delete(contract)

			// Drain buffered transfers that arrived during the snapshot window.
			l.pendingTransfersMu.Lock()
			buffered := l.pendingTransfers[contract]
			delete(l.pendingTransfers, contract)
			l.pendingTransfersMu.Unlock()

			if len(buffered) > 0 {
				l.logger.Info("Applying buffered transfers post-snapshot", zap.String("contract", contract), zap.Int("count", len(buffered)))
				for _, t := range buffered {
					if err := l.dbStore.UpdateOwnership(ctx, contract, t.to, t.tokenID); err != nil {
						l.logger.Error("Failed to apply buffered transfer", zap.String("contract", contract), zap.String("token", t.tokenID), zap.Error(err))
					}
				}
			}

		case <-ctx.Done():
			return
		}
	}
}

// ManuallyEnqueue pushes a contract into the discovery queue.
// forceResnap=true bypasses the knownContracts guard (used for startup recovery re-snapshots).
func (l *Listener) ManuallyEnqueue(contract string) bool {
	return l.enqueue(contract, false)
}

func (l *Listener) ForceResnap(contract string) bool {
	// Remove from known so the discovery loop re-runs a full snapshot.
	l.knownContracts.Delete(contract)
	return l.enqueue(contract, true)
}

func (l *Listener) enqueue(contract string, force bool) bool {
	if !force {
		if _, known := l.knownContracts.Load(contract); known {
			return false
		}
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
	return true
}
