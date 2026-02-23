package chain

import (
	"context"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

// Multicall3 common deployment address across chains
var Multicall3Address = common.HexToAddress("0xcA11bde05977b3631167028862bE2a173976CA11")

const multicall3ABI = `[{"inputs":[{"components":[{"internalType":"address","name":"target","type":"address"},{"internalType":"bool","name":"allowFailure","type":"bool"},{"internalType":"bytes","name":"callData","type":"bytes"}],"internalType":"struct Multicall3.Call3[]","name":"calls","type":"tuple[]"}],"name":"aggregate3","outputs":[{"components":[{"internalType":"bool","name":"success","type":"bool"},{"internalType":"bytes","name":"returnData","type":"bytes"}],"internalType":"struct Multicall3.Result[]","name":"returnData","type":"tuple[]"}],"stateMutability":"payable","type":"function"}]`

var parsedABI abi.ABI

func init() {
	var err error
	parsedABI, err = abi.JSON(strings.NewReader(multicall3ABI))
	if err != nil {
		panic(fmt.Sprintf("failed to parse multicall ABI: %v", err))
	}
}

type Call3 struct {
	Target       common.Address
	AllowFailure bool
	CallData     []byte
}

type Result struct {
	Success    bool
	ReturnData []byte
}

func Aggregate3(ctx context.Context, client *Client, calls []Call3) ([]Result, error) {
	if len(calls) == 0 {
		return nil, nil // Nothing to do
	}

	callData, err := parsedABI.Pack("aggregate3", calls)
	if err != nil {
		return nil, fmt.Errorf("failed to pack multicall aggregate3: %w", err)
	}

	msg := ethereum.CallMsg{
		To:   &Multicall3Address,
		Data: callData,
	}

	resultData, err := client.HTTP.CallContract(ctx, msg, nil)
	if err != nil {
		return nil, fmt.Errorf("multicall eth_call failed: %w", err)
	}

	results, err := parsedABI.Unpack("aggregate3", resultData)
	if err != nil {
		return nil, fmt.Errorf("failed to unpack multicall results: %w", err)
	}

	// Workaround for []struct mapping from abi unpack
	out := make([]Result, len(calls))
	
	// Unpack returns an array of interfaces. In our ABI, the output is a single array of tuples.
	if len(results) != 1 {
		return nil, fmt.Errorf("unexpected out length from multicall unpack: %d", len(results))
	}

	outSlice, ok := results[0].([]struct {
		Success    bool   `json:"success"`
		ReturnData []byte `json:"returnData"`
	})
	
	if !ok {
		return nil, fmt.Errorf("failed to cast multicall results to expected struct slice")
	}

	for i, r := range outSlice {
		out[i] = Result{
			Success:    r.Success,
			ReturnData: r.ReturnData,
		}
	}

	return out, nil
}

// ERC721 ABIs
const (
	erc721ABI = `[{"constant":true,"inputs":[{"name":"_owner","type":"address"}],"name":"balanceOf","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"_tokenId","type":"uint256"}],"name":"ownerOf","outputs":[{"name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"_tokenId","type":"uint256"}],"name":"tokenURI","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"name","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"symbol","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"totalSupply","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"bytes4","name":"interfaceId","type":"bytes4"}],"name":"supportsInterface","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"}]`
)

var ParsedERC721ABI abi.ABI

func init() {
	var err error
	ParsedERC721ABI, err = abi.JSON(strings.NewReader(erc721ABI))
	if err != nil {
		panic(fmt.Sprintf("failed to parse erc721 ABI: %v", err))
	}
}

// ABI Helpers
func BuildCall(target string, method string, args ...interface{}) (Call3, error) {
	data, err := ParsedERC721ABI.Pack(method, args...)
	if err != nil {
		return Call3{}, fmt.Errorf("pack err on %s: %w", method, err)
	}
	return Call3{
		Target:       common.HexToAddress(target),
		AllowFailure: true, // safe default for aggregate operations
		CallData:     data,
	}, nil
}

// DecodeAddress decodes a single `address` return value
func DecodeAddress(data []byte) (string, error) {
	if len(data) == 0 {
		return "", fmt.Errorf("empty return data")
	}
	res, err := ParsedERC721ABI.Unpack("ownerOf", data)
	if err != nil || len(res) == 0 {
		return "", fmt.Errorf("unpack ownerOf issue")
	}
	addr, ok := res[0].(common.Address)
	if !ok {
		return "", fmt.Errorf("failed typecast address")
	}
	return addr.Hex(), nil
}

// DecodeString decodes a single `string` return value
func DecodeString(method string, data []byte) (string, error) {
	if len(data) == 0 {
		return "", fmt.Errorf("empty string return data")
	}
	
	// special case for potentially unformatted bytes32 strings
	res, err := ParsedERC721ABI.Unpack(method, data)
	if err != nil || len(res) == 0 {
		// fall-through to literal byte string strip zeroes if fallback
		val := string(common.TrimRightZeroes(data))
		// this skips length prefix, so just fallback simple return
		return strings.TrimSpace(val), nil
	}
	str, ok := res[0].(string)
	if !ok {
		return "", fmt.Errorf("failed typecast string")
	}
	return str, nil
}

// DecodeUint256 decodes a single `uint256` return value
func DecodeUint256(method string, data []byte) (*big.Int, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty return uint256 data")
	}
	res, err := ParsedERC721ABI.Unpack(method, data)
	if err != nil || len(res) == 0 {
		return nil, fmt.Errorf("unpack %s uint issue", method)
	}
	val, ok := res[0].(*big.Int)
	if !ok {
		return nil, fmt.Errorf("failed typecast big.Int")
	}
	return val, nil
}

// SupportsERC721 returns an aggregate3 payload to check supportsInterface 
func CheckERC721SupportCall(target string) Call3 {
	var interfaceId [4]byte
	copy(interfaceId[:], common.FromHex("0x80ac58cd")) // ERC-721
	
	data, _ := ParsedERC721ABI.Pack("supportsInterface", interfaceId)
	return Call3{
		Target:       common.HexToAddress(target),
		AllowFailure: true,
		CallData:     data,
	}
}

// Transfer Event Topic filter
var TransferEventHash = crypto.Keccak256Hash([]byte("Transfer(address,address,uint256)"))
