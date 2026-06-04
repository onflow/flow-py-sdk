"""
Cross-SDK integration test.

Runs the Go program (tests/cross_sdk/main.go) via subprocess, parses its JSON output,
then makes the identical queries against Flow mainnet using the Python SDK and asserts
that all key fields match between the two SDKs.

Run standalone:
    python tests/cross_sdk/test_cross_sdk.py
"""

import asyncio
import json
import os
import subprocess
import sys

# Make sure the repo root is on the path when run directly.
_REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from flow_py_sdk import flow_client  # noqa: E402

MAINNET_HOST = "access.mainnet.nodes.onflow.org"
MAINNET_PORT = 9000

# Location of the Go program, relative to this file.
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_GO_MAIN = os.path.join(_THIS_DIR, "main.go")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def run_go_program() -> dict:
    """Run the Go program and return its parsed JSON output."""
    print("Running Go program …", flush=True)
    result = subprocess.run(
        ["go", "run", _GO_MAIN],
        cwd=_THIS_DIR,
        capture_output=True,
        text=True,
        timeout=120,
    )
    if result.returncode != 0:
        print("Go stderr:", result.stderr, file=sys.stderr)
        raise RuntimeError(f"Go program exited with code {result.returncode}: {result.stderr.strip()}")
    try:
        data = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Could not parse Go output as JSON: {result.stdout!r}") from exc
    return data


def _assert_eq(label: str, go_val, py_val) -> None:
    if go_val == py_val:
        print(f"  [PASS] {label}: {py_val!r}")
    else:
        print(f"  [FAIL] {label}")
        print(f"         Go  = {go_val!r}")
        print(f"         Py  = {py_val!r}")
        raise AssertionError(f"Mismatch on {label!r}: Go={go_val!r}, Py={py_val!r}")


# ---------------------------------------------------------------------------
# Main async logic
# ---------------------------------------------------------------------------

async def run_python_queries(go_data: dict) -> dict:
    """Query mainnet using the Python SDK for the same block / entities that Go used."""
    block_id_hex: str = go_data["block_id"]
    block_height: int = go_data["block_height"]
    collection_id_hex: str = go_data["collection_id"]
    tx_id_hex: str = go_data["tx_id"]

    block_id_bytes = bytes.fromhex(block_id_hex)
    collection_id_bytes = bytes.fromhex(collection_id_hex)
    tx_id_bytes = bytes.fromhex(tx_id_hex)

    print(f"\nQuerying Python SDK for block height {block_height} …", flush=True)

    async with flow_client(host=MAINNET_HOST, port=MAINNET_PORT) as client:
        # Step 1: Get the block by the same height the Go SDK used.
        block = await client.get_block_by_height(height=block_height)

        # Step 2: Get the collection.
        collection = await client.get_collection_by_i_d(id=collection_id_bytes)

        # Step 3: Get the transaction.
        tx = await client.get_transaction(id=tx_id_bytes)

        # Step 4: Get the transaction result.
        tx_result = await client.get_transaction_result(id=tx_id_bytes)

    # Convert collection guarantee IDs from the block to hex strings.
    block_collection_ids = [
        cg.collection_id.hex() for cg in block.collection_guarantees
    ]

    # Collection transaction IDs as hex.
    col_tx_ids = [tid.hex() for tid in collection.transaction_ids]

    return {
        "block_id": block.id.hex(),
        "block_height": block.height,
        "num_collections": len(block.collection_guarantees),
        "block_collection_ids": block_collection_ids,
        "collection_id_in_block": collection_id_hex in block_collection_ids,
        "collection_tx_ids": col_tx_ids,
        "tx_id": tx_id_bytes.hex(),
        "payer": tx.payer.hex(),
        "tx_status": int(tx_result.status),
        "tx_error_message": tx_result.error_message,
    }


def compare_and_assert(go_data: dict, py_data: dict) -> None:
    """Compare Go and Python results, printing pass/fail for each field."""
    print("\n--- Comparison ---")

    # Block
    _assert_eq("block.id", go_data["block_id"], py_data["block_id"])
    _assert_eq("block.height", go_data["block_height"], py_data["block_height"])
    _assert_eq("block.num_collections", go_data["num_collections"], py_data["num_collections"])

    # Collection guarantee appears in block
    if not py_data["collection_id_in_block"]:
        raise AssertionError(
            f"Collection ID {go_data['collection_id']!r} from Go not found in "
            f"Python block's collection guarantees: {py_data['block_collection_ids']}"
        )
    print(f"  [PASS] collection_id present in block.collection_guarantees: {go_data['collection_id']!r}")

    # Collection transaction IDs
    _assert_eq("collection.tx_ids", go_data["collection_tx_ids"], py_data["collection_tx_ids"])

    # Transaction
    _assert_eq("tx.id", go_data["tx_id"], py_data["tx_id"])
    _assert_eq("tx.payer", go_data["payer"], py_data["payer"])

    # Transaction result
    _assert_eq("tx_result.status (int)", go_data["tx_status"], py_data["tx_status"])
    _assert_eq("tx_result.error_message", go_data["tx_error_message"], py_data["tx_error_message"])


def main() -> None:
    # Step A: Run the Go program and parse its output.
    go_data = run_go_program()

    print("\n--- Go SDK output ---")
    print(json.dumps(go_data, indent=2))

    # Step B: Run the Python SDK queries.
    py_data = asyncio.run(run_python_queries(go_data))

    print("\n--- Python SDK output ---")
    print(json.dumps(py_data, indent=2))

    # Step C: Assert all fields match.
    compare_and_assert(go_data, py_data)

    print("\n=== ALL ASSERTIONS PASSED ===")


if __name__ == "__main__":
    main()
