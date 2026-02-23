# Stage 1: Build the Go binaries
FROM golang:1.24-alpine AS builder

# Install SSL root certificates
RUN apk --no-cache add ca-certificates

WORKDIR /app

# Copy dependency files first to cache module downloads
COPY go.mod go.sum ./
RUN go mod download

# Copy the source code
COPY . .

# Build the binaries with CGO disabled for scratch compatibility
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o /bin/indexer ./cmd/indexer
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o /bin/api ./cmd/api

# Stage 2: Create the minimal runtime container
FROM alpine:latest

# Certificates are required for Ethereum RPC and IPFS HTTPS requests
RUN apk --no-cache add ca-certificates

WORKDIR /app

# Copy the compiled binaries from the builder stage
COPY --from=builder /bin/indexer /app/indexer
COPY --from=builder /bin/api /app/api

# Expose the API port (Configurable via ENV normally, exposing default 3000)
EXPOSE 3000

# By default, we can set the entrypoint to the API, but docker-compose will override this to run both
CMD ["/app/api"]
