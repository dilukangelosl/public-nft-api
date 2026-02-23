package chain

import (
	"context"
	"fmt"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
)

type Client struct {
	WSS  *ethclient.Client
	HTTP *ethclient.Client
	Raw  *rpc.Client
}

func Connect(ctx context.Context, wssURL, httpURL string) (*Client, error) {
	// Initialize the raw RPC client (used for low level eth_call and batch operations)
	rawRPC, err := rpc.DialContext(ctx, httpURL)
	if err != nil {
		return nil, fmt.Errorf("failed to dial raw RPC: %w", err)
	}

	httpClient := ethclient.NewClient(rawRPC)

	// WSS connection for subscriptions
	wssClient, err := ethclient.DialContext(ctx, wssURL)
	if err != nil {
		return nil, fmt.Errorf("failed to dial WSS client: %w", err)
	}

	return &Client{
		WSS:  wssClient,
		HTTP: httpClient,
		Raw:  rawRPC,
	}, nil
}
