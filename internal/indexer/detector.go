package indexer

import (
	"context"

	"github.com/dilukangelosl/public-nft-api/internal/chain"
)

// ValidateERC721 performs a single Multicall3 `supportsInterface` check on a potential contract.
func ValidateERC721(ctx context.Context, eth *chain.Client, address string) (bool, error) {
	call := chain.CheckERC721SupportCall(address)
	res, err := chain.Aggregate3(ctx, eth, []chain.Call3{call})
	if err != nil {
		return false, err
	}
	
	if len(res) == 0 {
		return false, nil // generic error or empty
	}

	// supportsInterface returns a bool true/false
	// ReturnData corresponds strictly to raw bool encoding
	return len(res[0].ReturnData) > 0 && res[0].ReturnData[len(res[0].ReturnData)-1] == 1, nil
}
