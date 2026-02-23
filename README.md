# Public NFT Indexer API

This repository contains a high-performance Ethereum NFT indexer and public API written in Go. The system consists of a background daemon capturing ERC-721 Transfer events and a public-facing REST API.

## Architecture

1. **Indexer Daemon**: Listens to Ethereum WebSocket events and uses Multicall3 to quickly snapshot current ownership for newly discovered collections.
2. **REST API**: Built with Chi router and Ristretto caching. Serves detailed paginated token data, cross-collection owner lookups, and manual indexing queues.
3. **Storage**: PostgreSQL handles relational schema structure of collections, tokens, and metadata.
4. **Metadata Dispatcher**: Scalable worker pools iterating over multiple fallback IPFS gateways (Cloudflare, Pinata, etc.) to resolve IPFS/HTTP/Base64 URIs into JSON metadata.

## Prerequisites

- Go 1.24 or later
- PostgreSQL database
- Ethereum RPC Node with WebSocket support (e.g. Alchemy)

## Configuration

Duplicate `.env.example` to `.env` and fill in the required variables.

```env
DATABASE_URL=postgres://user:password@localhost:5432/nftdb
ALCHEMY_WSS_URL=wss://eth-mainnet.g.alchemy.com/v2/KEY
ALCHEMY_HTTP_URL=https://eth-mainnet.g.alchemy.com/v2/KEY
PORT=3000
IPFS_GATEWAYS=https://cloudflare-ipfs.com/ipfs/,https://gateway.pinata.cloud/ipfs/
```

## Running the Application

### Method 1: Bare Metal

First, run the database migrations in `migrations/001_init.sql` against your PostgreSQL instance.

Start the background indexer:
```bash
go run ./cmd/indexer
```

Start the API Server:
```bash
go run ./cmd/api
```

The API will expose documentation by default at the root URL paths.

### Method 2: Docker

```bash
docker compose up --build -d
```

## Manual Indexing

You can queue an unindexed collection dynamically through the API without waiting for a blockchain transfer event.

```bash
curl -X POST http://localhost:3000/v1/collections \
  -H "Content-Type: application/json" \
  -d '{"address":"0x..."}'
```
