package metadata

import (
	"context"
	"sync"
	"time"

	"github.com/dilukangelosl/public-nft-api/internal/chain"
	"github.com/dilukangelosl/public-nft-api/internal/store"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
)

const (
	WorkersPerCollection   = 5
	MaxMetadataGoroutines  = 100 
)

// Dispatcher acts as the engine managing map/queue life-cycle execution of separate pools
type Dispatcher struct {
	eth          *chain.Client
	db           *store.Store
	logger       *zap.Logger
	QueueMap     map[string]chan string
	Mutex        *sync.RWMutex
	gateways     []string
	GlobalSem    *semaphore.Weighted
}

func NewDispatcher(eth *chain.Client, db *store.Store, log *zap.Logger, ips []string) *Dispatcher {
	return &Dispatcher{
		eth:       eth,
		db:        db,
		logger:    log,
		QueueMap:  make(map[string]chan string),
		Mutex:     &sync.RWMutex{},
		gateways:  ips,
		GlobalSem: semaphore.NewWeighted(MaxMetadataGoroutines),
	}
}

func (d *Dispatcher) Start(ctx context.Context) {
	d.logger.Info("Metadata Dispatcher started")

	// The dispatcher polls the QueueMap periodically looking for non-empty queues that don't have active managers.
	// We'll maintain a simple tracking map of "activePools"
	activePools := make(map[string]bool)

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			d.logger.Info("Dispatcher context killed")
			return
		case <-ticker.C:
			d.Mutex.RLock()
			// find contracts with items to process
			for contract, channel := range d.QueueMap {
				if len(channel) > 0 {
					if !activePools[contract] {
						// Promote contract channel to an active pool
						activePools[contract] = true
						go d.LaunchPool(ctx, contract, channel, activePools)
					}
				}
			}
			d.Mutex.RUnlock()
		}
	}
}

// LaunchPool starts isolated goroutines focused only on draining `ch`
func (d *Dispatcher) LaunchPool(ctx context.Context, contract string, ch chan string, activeRegistry map[string]bool) {
	d.logger.Info("Launching Worker Pool for Collection", zap.String("contract", contract))

	var wg sync.WaitGroup

	for i := 0; i < WorkersPerCollection; i++ {
		wg.Add(1)

		go func(workerId int) {
			defer wg.Done()
			
			// Try reserving from global caps 
			err := d.GlobalSem.Acquire(ctx, 1)
			if err != nil { // ctx expired or interrupted
				return
			}
			defer d.GlobalSem.Release(1)

			gatewayIdx := 0 // independent gateway index starting state for this worker

			// Drain logic via manual timeout
			// A clean way to naturally die off is looping until channel stays completely dry for arbitrary 2 mins
			idleTimer := time.NewTimer(2 * time.Minute)
			defer idleTimer.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case tokenID := <-ch:
					// Drain items
					if !idleTimer.Stop() {
						<-idleTimer.C // drain
					}
					idleTimer.Reset(2 * time.Minute)

					d.processJobWithRetries(ctx, contract, tokenID, &gatewayIdx)

				case <-idleTimer.C:
					// Starved timeout triggered, end this specific worker
					return
				}
			}
		}(i)
	}

	// This wrapper waits for all attached child goroutines to finish (meaning channel stayed completely idle for 2 min).
	// We then scrub our active mapping.
	wg.Wait()

	d.logger.Info("Worker Pool naturally drained and completed", zap.String("contract", contract))
	d.Mutex.Lock()
	activeRegistry[contract] = false

	// Cleanup QueueMap if strictly empty to avoid massive memory leaks over millions of contracts
	// Ensure we lock so we do not race against a new insert adding to channel while deleting
	if len(ch) == 0 {
		delete(d.QueueMap, contract)
	}
	d.Mutex.Unlock()
}
