package metadata

import (
	"context"
	"time"

	"go.uber.org/zap"
)

// processJobWithRetries handles up to 3 full gateway cycles for a failing IPFS request
func (d *Dispatcher) processJobWithRetries(ctx context.Context, contract string, tokenID string, gatewayIdx *int) {
	d.logger.Debug("Processing metadata job", zap.String("contract", contract), zap.String("tokenId", tokenID))

	maxCycles := 3
	gatewaysTriedInCurrentCycle := 0
	cyclesCompleted := 0

	for {
		// Attempt full resolution and fetch
		meta, newIdx, err := FetchMetadata(ctx, d.eth, contract, tokenID, d.gateways, *gatewayIdx, d.logger)
		
		*gatewayIdx = newIdx // Update external tracker whether success or fail to round-robin naturally

		if err == nil {
			// Success! Insert metadata unconditionally
			err := d.db.UpsertMetadata(ctx, meta)
			if err != nil {
				d.logger.Error("Failed to persist resolved metadata", zap.Error(err), zap.String("contract", contract))
			}
			return
		}

		// Handle error condition - determine if unrecoverable or retry-able
		d.logger.Warn("Fetch metadata failed", zap.Error(err), zap.String("contract", contract), zap.String("token", tokenID))

		// Cycle tracking logic
		gatewaysTriedInCurrentCycle++
		if gatewaysTriedInCurrentCycle >= len(d.gateways) {
			cyclesCompleted++
			gatewaysTriedInCurrentCycle = 0
			
			if cyclesCompleted >= maxCycles {
				d.logger.Error("Max metadata gateway rotation cycles reached, giving up on token",
					zap.String("contract", contract),
					zap.String("token", tokenID),
				)
				return
			}
			
			// Full cycle complete without success, sleep momentarily before hammering first gateway again
			select {
			case <-time.After(10 * time.Second):
			case <-ctx.Done():
				return
			}
		}
	}
}
