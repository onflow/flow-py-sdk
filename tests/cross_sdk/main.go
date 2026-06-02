// Cross-SDK integration test — Go side.
//
// Queries Flow mainnet using the Go SDK and prints a JSON object to stdout
// with the key fields that the Python test will compare against the Python SDK.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/onflow/flow-go-sdk"
	"github.com/onflow/flow-go-sdk/access/grpc"
)

const mainnetHost = "access.mainnet.nodes.onflow.org:9000"

// Result is the JSON payload written to stdout.
type Result struct {
	// Block fields
	BlockID     string `json:"block_id"`
	BlockHeight uint64 `json:"block_height"`
	NumCollections int `json:"num_collections"`

	// Collection fields
	CollectionID    string   `json:"collection_id"`
	CollectionTxIDs []string `json:"collection_tx_ids"`

	// Transaction fields
	TxID  string `json:"tx_id"`
	Payer string `json:"payer"`

	// Transaction result fields
	TxStatus       int    `json:"tx_status"`
	TxStatusString string `json:"tx_status_string"`
	TxErrorMessage string `json:"tx_error_message"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	ctx := context.Background()

	client, err := grpc.NewClient(mainnetHost)
	if err != nil {
		return fmt.Errorf("creating gRPC client: %w", err)
	}

	// Step 1: Get the latest sealed block.
	latestBlock, err := client.GetLatestBlock(ctx, true)
	if err != nil {
		return fmt.Errorf("GetLatestBlock: %w", err)
	}

	// Walk back until we find a block that has at least one collection guarantee.
	// Some blocks on mainnet have no collections.
	block := latestBlock
	const maxWalkback = 200
	for i := 0; i < maxWalkback; i++ {
		if len(block.CollectionGuarantees) > 0 {
			break
		}
		if block.Height == 0 {
			return fmt.Errorf("reached block 0 without finding a block with collections")
		}
		block, err = client.GetBlockByHeight(ctx, block.Height-1)
		if err != nil {
			return fmt.Errorf("GetBlockByHeight(%d): %w", block.Height-1, err)
		}
	}
	if len(block.CollectionGuarantees) == 0 {
		return fmt.Errorf("could not find a block with collections within %d blocks of the latest", maxWalkback)
	}

	// Step 2: Get a collection from the block.
	// Try each collection guarantee until we find one that has at least one transaction.
	var collection *flow.Collection
	var collectionID flow.Identifier
	for _, cg := range block.CollectionGuarantees {
		col, err := client.GetCollection(ctx, cg.CollectionID)
		if err != nil {
			// skip if not available
			continue
		}
		if len(col.TransactionIDs) > 0 {
			collection = col
			collectionID = cg.CollectionID
			break
		}
	}
	if collection == nil {
		return fmt.Errorf("no non-empty collection found in block %d", block.Height)
	}

	// Step 3: Get the first transaction from the collection.
	txID := collection.TransactionIDs[0]
	tx, err := client.GetTransaction(ctx, txID)
	if err != nil {
		return fmt.Errorf("GetTransaction(%s): %w", txID, err)
	}

	// Step 4: Get the transaction result.
	txResult, err := client.GetTransactionResult(ctx, txID)
	if err != nil {
		return fmt.Errorf("GetTransactionResult(%s): %w", txID, err)
	}

	// Build collection tx ID list.
	colTxIDs := make([]string, len(collection.TransactionIDs))
	for i, id := range collection.TransactionIDs {
		colTxIDs[i] = id.Hex()
	}

	errMsg := ""
	if txResult.Error != nil {
		errMsg = txResult.Error.Error()
	}

	result := Result{
		BlockID:        block.ID.Hex(),
		BlockHeight:    block.Height,
		NumCollections: len(block.CollectionGuarantees),

		CollectionID:    collectionID.Hex(),
		CollectionTxIDs: colTxIDs,

		TxID:  txID.Hex(),
		Payer: tx.Payer.Hex(),

		TxStatus:       int(txResult.Status),
		TxStatusString: txResult.Status.String(),
		TxErrorMessage: errMsg,
	}

	enc, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return fmt.Errorf("json marshal: %w", err)
	}
	fmt.Println(string(enc))
	return nil
}
