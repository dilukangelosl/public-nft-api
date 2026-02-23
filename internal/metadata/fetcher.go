package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"time"

	"github.com/dilukangelosl/public-nft-api/internal/chain"
	"github.com/dilukangelosl/public-nft-api/internal/store"
	"go.uber.org/zap"
)

// FetchMetadata retrieves, decodes, and standardizes ERC-721 token metadata.
// It accepts the gateway list and modifies it locally using its worker's rotation state.
func FetchMetadata(
	ctx context.Context,
	eth *chain.Client,
	contract, tokenID string,
	gateways []string,
	gatewayIdx int,
	logger *zap.Logger,
) (store.Metadata, int, error) {

	// 1. Fetch tokenURI from contract directly
	tid, ok := new(big.Int).SetString(tokenID, 10)
	if !ok {
		return store.Metadata{}, gatewayIdx, fmt.Errorf("invalid token id format: %s", tokenID)
	}

	call, _ := chain.BuildCall(contract, "tokenURI", tid)
	res, err := chain.Aggregate3(ctx, eth, []chain.Call3{call})
	if err != nil || len(res) == 0 || !res[0].Success {
		// Log as unrecoverable at this level if token is outright missing URI
		return store.Metadata{}, gatewayIdx, fmt.Errorf("failed tokenURI read for %s-%s: %w", contract, tokenID, err)
	}

	uri, err := chain.DecodeString("tokenURI", res[0].ReturnData)
	if err != nil {
		return store.Metadata{}, gatewayIdx, fmt.Errorf("failed unpacking string from returnData: %w", err)
	}

	if uri == "" {
		// Empty URI is valid state technically according to specific cases but doesn't produce metadata
		return store.Metadata{Contract: contract, TokenID: tokenID}, gatewayIdx, nil
	}

	// 2. Fetch or Decode Data Payload
	var b []byte
	var isInline bool
	targetURL, isInline := ResolveURI(uri, gateways[gatewayIdx])

	if isInline {
		b, err = ExtractBase64(targetURL)
		if err != nil {
			return store.Metadata{}, gatewayIdx, fmt.Errorf("failed decoding base64 payload: %w", err)
		}
	} else if targetURL != "" {
		b, gatewayIdx, err = HTTPFetchWithRotator(ctx, targetURL, gateways, gatewayIdx, logger)
		if err != nil {
			return store.Metadata{}, gatewayIdx, fmt.Errorf("failed HTTP fetch logic: %w", err)
		}
	} else {
		// Unsupported or unresolvable URI gracefully skips
		return store.Metadata{Contract: contract, TokenID: tokenID}, gatewayIdx, nil
	}

	// 3. Unmarshal loosely formatted structures
	var output store.Metadata
	output.Contract = contract
	output.TokenID = tokenID

	// Intercept generic JSON mapping
	var generic map[string]interface{}
	if err := json.Unmarshal(b, &generic); err == nil {
		if val, ok := generic["name"].(string); ok {
			output.Name = val
		}
		if val, ok := generic["description"].(string); ok {
			output.Description = val
		}
		if val, ok := generic["image"].(string); ok {
			output.Image = val
		}

		if attrs, ok := generic["attributes"]; ok {
			output.Attributes = attrs // standard pass-through of any struct array (e.g. trait_types)
		}
	} else {
		logger.Warn("Failed parsing token JSON metadata gracefully, saving missing schema details", zap.String("id", tokenID), zap.String("uri", uri))
	}

	return output, gatewayIdx, nil
}

// HTTPFetchWithRotator attempts to fetch the URL but implements rotating logic inside.
// NOTE: It tries once based on provided `gatewayIdx`. Retry loops are managed up-level by the worker to track cross-job counts.
func HTTPFetchWithRotator(ctx context.Context, target string, gateways []string, startIdx int, logger *zap.Logger) ([]byte, int, error) {
	client := http.Client{
		Timeout: 10 * time.Second,
	}
	
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, startIdx, err
	}

	resp, err := client.Do(req)
	if err != nil {
		// rotate index instantly on network failure
		return nil, (startIdx + 1) % len(gateways), fmt.Errorf("network error fetching URI: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
		return nil, (startIdx + 1) % len(gateways), fmt.Errorf("bad status %d received", resp.StatusCode)
	}

	if resp.StatusCode >= 400 && resp.StatusCode < 500 {
		return nil, startIdx, fmt.Errorf("unrecoverable 404/403 status %d received", resp.StatusCode)
	}

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, startIdx, fmt.Errorf("error reading body payload: %w", err)
	}
	return b, startIdx, nil
}

