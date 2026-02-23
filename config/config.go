package config

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

type Config struct {
	DatabaseURL  string
	AlchemyWSS   string
	AlchemyHTTP  string
	IPFSGateways []string
	Port         string
}

func Load() (*Config, error) {
	viper.AutomaticEnv()
	viper.SetDefault("PORT", "8080")
	viper.SetDefault("IPFS_GATEWAYS", "https://cloudflare-ipfs.com/ipfs/,https://gateway.pinata.cloud/ipfs/,https://nftstorage.link/ipfs/,https://w3s.link/ipfs/,https://4everland.io/ipfs/")

	viper.SetConfigFile(".env")
	_ = viper.ReadInConfig() // ignore missing .env file, env vars are fine

	dbURL := viper.GetString("DATABASE_URL")
	if dbURL == "" {
		return nil, fmt.Errorf("DATABASE_URL is required")
	}

	wssURL := viper.GetString("ALCHEMY_WSS_URL")
	if wssURL == "" {
		return nil, fmt.Errorf("ALCHEMY_WSS_URL is required")
	}

	httpURL := viper.GetString("ALCHEMY_HTTP_URL")
	if httpURL == "" {
		return nil, fmt.Errorf("ALCHEMY_HTTP_URL is required")
	}

	gatewaysStr := viper.GetString("IPFS_GATEWAYS")
	gateways := strings.Split(gatewaysStr, ",")
	var validGateways []string
	for _, g := range gateways {
		g = strings.TrimSpace(g)
		if g != "" {
			validGateways = append(validGateways, g)
		}
	}

	return &Config{
		DatabaseURL:  dbURL,
		AlchemyWSS:   wssURL,
		AlchemyHTTP:  httpURL,
		IPFSGateways: validGateways,
		Port:         viper.GetString("PORT"),
	}, nil
}
