package indexer

import (
	"context"
	"fmt"
	"math/big"

	"github.com/dilukangelosl/public-nft-api/internal/chain"
	"go.uber.org/zap"
)

// DetectStartIndex determines whether a collection starts at Token ID 0 or 1.
// It performs a single Multicall3 request executing `tokenURI(0)` and `tokenURI(1)`
// and checking which call(s) succeed (don't revert).
func DetectStartIndex(ctx context.Context, ethClient *chain.Client, contractAddr string, logger *zap.Logger) (int, bool, error) {
	call0, err := chain.BuildCall(contractAddr, "tokenURI", big.NewInt(0))
	if err != nil {
		return 0, false, fmt.Errorf("failed creating tokenURI(0) call: %w", err)
	}

	call1, err := chain.BuildCall(contractAddr, "tokenURI", big.NewInt(1))
	if err != nil {
		return 0, false, fmt.Errorf("failed creating tokenURI(1) call: %w", err)
	}

	calls := []chain.Call3{call0, call1}

	results, err := chain.Aggregate3(ctx, ethClient, calls)
	if err != nil {
		return 0, false, fmt.Errorf("tokenURI probe multicall failed: %w", err)
	}

	if len(results) != 2 {
		return 0, false, fmt.Errorf("unexpected multicall result size: %d", len(results))
	}

	success0 := results[0].Success
	success1 := results[1].Success

	// IF result_0 did NOT revert -> RETURN 0
	if success0 {
		return 0, false, nil
	}

	// IF result_0 reverted -> RETURN 1
	if !success0 && success1 {
		return 1, false, nil
	}

	// IF BOTH reverted
	logger.Warn("Both tokenURI(0) and tokenURI(1) reverted during probe. Defaulting to index 1.",
		zap.String("contract", contractAddr),
	)
	return 1, true, nil
}
