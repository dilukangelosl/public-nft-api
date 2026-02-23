package metadata

import (
	"encoding/base64"
	"strings"
)

// ResolveURI translates an ipfs://, http:// or generic data: application URI into a usable HTTP target.
// It uses gateway parameter to construct active resolving URLs.
func ResolveURI(uri, activeGateway string) (string, bool) {
	if strings.HasPrefix(uri, "ipfs://") {
		// Replace ipfs:// prefix with active gateway
		hash := strings.TrimPrefix(uri, "ipfs://")
		return activeGateway + hash, false // returns true ONLY if it is inline Base64 data not needing HTTP fetch
	}

	if strings.HasPrefix(uri, "http://") || strings.HasPrefix(uri, "https://") {
		return uri, false
	}

	// data:application/json;base64,
	if strings.HasPrefix(uri, "data:application/json;base64,") {
		payload := strings.TrimPrefix(uri, "data:application/json;base64,")
		return payload, true
	}

	// Check generic data base64 (sometime seen bare base64 headers)
	if strings.HasPrefix(uri, "data:base64,") {
		payload := strings.TrimPrefix(uri, "data:base64,")
		return payload, true
	}

	return "", false
}

// ExtractBase64 returns raw decoded bytes from inline Base64 json.
func ExtractBase64(b64 string) ([]byte, error) {
	decoded, err := base64.StdEncoding.DecodeString(b64)
	if err != nil { // try URL encoding if Std fails
		return base64.URLEncoding.DecodeString(b64)
	}
	return decoded, nil
}
