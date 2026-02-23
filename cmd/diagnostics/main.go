package main

import (
	"context"
	"fmt"
	"log"

	"github.com/dilukangelosl/public-nft-api/config"
	"github.com/dilukangelosl/public-nft-api/internal/chain"
	"github.com/dilukangelosl/public-nft-api/internal/indexer"
	"github.com/dilukangelosl/public-nft-api/internal/store"
	"go.uber.org/zap"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatal(err)
	}

	eth, err := chain.Connect(context.Background(), cfg.AlchemyWSS, cfg.AlchemyHTTP)
	if err != nil {
		log.Fatal(err)
	}

	contract := "0x91417bd88af5071ccea8d3bf3af410660e356b06"
	is721, err := indexer.ValidateERC721(context.Background(), eth, contract)
	fmt.Printf("isERC721: %v, error: %v\n", is721, err)

	db, err := store.NewStore(context.Background(), cfg.DatabaseURL)
	if err != nil {
		log.Fatal(err)
	}

	logger, _ := zap.NewDevelopment()
	block, err := eth.HTTP.BlockNumber(context.Background())
	fmt.Printf("Latest Block: %d Error: %v\n", block, err)

	err = indexer.ProcessSnapshot(context.Background(), eth, db, contract, block, logger, nil, nil)
	fmt.Printf("ProcessSnapshot Error: %v\n", err)
}
