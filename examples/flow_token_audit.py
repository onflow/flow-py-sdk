"""
flow_token_audit.py — Walk sealed blocks and audit FLOW token transfers
=======================================================================

This example demonstrates the new bulk block-transaction APIs added in the
flow-py-sdk and shows how a chain-auditing reconciliation loop can be built
efficiently.

New APIs highlighted
--------------------
  client.get_transactions_by_block_id(block_id)          → list[Transaction]
  client.get_transaction_results_by_block_id(block_id)   → list[TransactionResultResponse]
  client.get_transaction_result_by_index(block_id, i)    → TransactionResultResponse
  client.get_system_transaction_result(block_id)         → TransactionResultResponse

Old pattern — O(C + T) API calls per block
  block
  └─ for each collection:        get_collection_by_i_d(collection_id)
     └─ for each tx_id:          get_transaction(tx_id)
                                 get_transaction_result(tx_id)

New pattern — 3 API calls per block regardless of transaction count
  block
  ├─ get_transactions_by_block_id(block.id)          # all Tx metadata
  ├─ get_transaction_results_by_block_id(block.id)   # all events / statuses
  └─ get_system_transaction_result(block.id)         # protocol-level events

Event guidance (confirmed with Flow team, May 2026)
----------------------------------------------------
For FLOW token reconciliation use the LEGACY events only:
  A.1654653399040a61.FlowToken.TokensDeposited(amount: UFix64, to: Address?)
  A.1654653399040a61.FlowToken.TokensWithdrawn(amount: UFix64, from: Address?)

The newer FungibleToken.Deposited / Withdrawn events have optional `from`/`to`
fields that can be nil for transient vaults (bridge, staking rewards, DEX
routers). The legacy events' address fields are always populated for real
account addresses, making them safe for custody-address reconciliation.

SDK
---
This example requires flow-py-sdk v2.0.3 or later:
  https://github.com/onflow/flow-py-sdk

Install
-------
  pip install flow-py-sdk==2.0.3

Usage
-----
  # Scan the 10 most recent sealed blocks on mainnet
  python flow_token_audit.py

  # Scan 20 blocks starting at a specific height, flag two custody addresses
  python flow_token_audit.py \\
      --blocks 20 \\
      --start-height 105000000 \\
      --custody-address 0x1234567890abcdef \\
      --custody-address 0xfedcba0987654321
"""

import argparse
import asyncio
import json
import logging
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional

from flow_py_sdk import flow_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("flow-audit")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MAINNET_HOST = "access.mainnet.nodes.onflow.org"
MAINNET_PORT = 9000

# Legacy FLOW token events — the recommended integration point for exchanges.
EVENT_TOKENS_DEPOSITED = "A.1654653399040a61.FlowToken.TokensDeposited"
EVENT_TOKENS_WITHDRAWN = "A.1654653399040a61.FlowToken.TokensWithdrawn"


# ---------------------------------------------------------------------------
# Data containers
# ---------------------------------------------------------------------------

@dataclass
class Transfer:
    """One deposit or withdrawal event parsed from a transaction result."""
    block_height: int
    tx_id: str           # hex string; "system" for the system transaction
    tx_index: int        # position within the block (-1 for system tx)
    event_index: int     # position within the transaction
    amount: Decimal      # FLOW amount (human-readable, e.g. 1.50000000)
    address: Optional[str]  # hex address; None when the vault has no owner


@dataclass
class BlockAudit:
    height: int
    block_id: str
    num_transactions: int
    deposits: list[Transfer] = field(default_factory=list)
    withdrawals: list[Transfer] = field(default_factory=list)
    system_tx_scanned: bool = False


# ---------------------------------------------------------------------------
# Event payload parsing
# ---------------------------------------------------------------------------

def _parse_optional_address(cadence_field: dict) -> Optional[str]:
    """
    Extract the address string from a Cadence Optional<Address> JSON value,
    or return None when the optional is nil (transient vault with no owner).

    Cadence JSON shape:
      {"type": "Optional", "value": {"type": "Address", "value": "0x..."}}
      {"type": "Optional", "value": null}
    """
    if cadence_field.get("type") != "Optional":
        return None
    inner = cadence_field.get("value")
    if inner is None:
        return None
    return inner.get("value")   # address hex string


def _parse_ufix64(cadence_field: dict) -> Decimal:
    """
    Extract a Decimal from a Cadence UFix64 JSON value.

    Cadence JSON shape: {"type": "UFix64", "value": "1.50000000"}

    Per Flow team guidance: amount is declared as UFix64
    (non-optional), so it is always present. A missing or null amount
    indicates a client decoding bug, not a legitimate protocol state.
    """
    return Decimal(cadence_field["value"])


def parse_flow_token_event(
    event_type: str,
    payload_bytes: bytes,
    block_height: int,
    tx_id: str,
    tx_index: int,
    event_index: int,
) -> Optional[Transfer]:
    """
    Parse a FlowToken.TokensDeposited or FlowToken.TokensWithdrawn event
    payload (raw Cadence ABI-encoded JSON bytes) into a Transfer record.

    Returns None if the event type is not one we care about, or if parsing
    fails (logged as a warning rather than raising).

    Field layout for both events:
      fields[0]  amount : UFix64     — always non-null
      fields[1]  to / from : Address? — nil for transient vaults
    """
    if event_type not in (EVENT_TOKENS_DEPOSITED, EVENT_TOKENS_WITHDRAWN):
        return None

    try:
        payload = json.loads(payload_bytes.decode("utf-8"))
        fields = payload["value"]["fields"]
        amount = _parse_ufix64(fields[0]["value"])
        address = _parse_optional_address(fields[1]["value"])
    except Exception as exc:
        log.warning(
            f"Could not parse {event_type} payload in tx {tx_id}: {exc}"
        )
        return None

    return Transfer(
        block_height=block_height,
        tx_id=tx_id,
        tx_index=tx_index,
        event_index=event_index,
        amount=amount,
        address=address,
    )


def collect_transfers(
    events,
    block_height: int,
    tx_id: str,
    tx_index: int,
) -> tuple[list[Transfer], list[Transfer]]:
    """Scan an event list and return (deposits, withdrawals)."""
    deposits: list[Transfer] = []
    withdrawals: list[Transfer] = []
    for event in events:
        transfer = parse_flow_token_event(
            event.type,
            event.payload,
            block_height,
            tx_id,
            tx_index,
            event.event_index,
        )
        if transfer is None:
            continue
        if event.type == EVENT_TOKENS_DEPOSITED:
            deposits.append(transfer)
        else:
            withdrawals.append(transfer)
    return deposits, withdrawals


# ---------------------------------------------------------------------------
# Block audit — the core of the example
# ---------------------------------------------------------------------------

async def audit_block(client, block) -> BlockAudit:
    """
    Scan a single sealed block and return all FLOW token transfer events.

    Key API calls
    -------------
    get_transactions_by_block_id   — returns every Transaction in the block
                                     (including those not reachable via
                                     collection queries, e.g. system txs)
    get_transaction_results_by_block_id — one result per transaction,
                                     containing the events and status
    get_system_transaction_result  — the protocol-level transaction that runs
                                     at the end of every block (epoch logic,
                                     fee distribution, reward minting)
    """
    block_id = block.id
    audit = BlockAudit(
        height=block.height,
        block_id=block_id.hex(),
        num_transactions=0,
    )

    # ── NEW API: bulk-fetch every transaction in the block ─────────────────
    # This replaces the old pattern of iterating collections → get_collection
    # → iterating tx_ids → individual get_transaction calls.
    transactions = await client.get_transactions_by_block_id(block_id=block_id)
    audit.num_transactions = len(transactions)

    # ── NEW API: bulk-fetch every transaction result in the block ──────────
    # This replaces the old per-tx get_transaction_result calls.
    # Results are returned in execution order, aligned with `transactions`.
    tx_results = await client.get_transaction_results_by_block_id(block_id=block_id)

    for i, tx_result in enumerate(tx_results):
        # Skip transactions that failed with an error.
        if tx_result.error_message:
            log.debug(f"  [block {block.height}] tx[{i}] failed: {tx_result.error_message}")
            continue

        # event.transaction_id carries the actual tx hash from the node.
        tx_id_hex = (
            tx_result.events[0].transaction_id.hex()
            if tx_result.events
            else f"block_{block.height}_tx_{i}"
        )

        deposits, withdrawals = collect_transfers(
            tx_result.events, block.height, tx_id_hex, i
        )
        audit.deposits.extend(deposits)
        audit.withdrawals.extend(withdrawals)

    # ── NEW API: system transaction result ─────────────────────────────────
    # The system transaction is NOT included in get_transaction_results_by_block_id.
    # It handles protocol-level FLOW movements: epoch rewards, service fees,
    # staking payouts. These can be large and must not be missed.
    try:
        sys_result = await client.get_system_transaction_result(block_id=block_id)
        audit.system_tx_scanned = True

        deposits, withdrawals = collect_transfers(
            sys_result.events, block.height, "system", -1
        )
        audit.deposits.extend(deposits)
        audit.withdrawals.extend(withdrawals)
    except Exception as exc:
        # Not every block has a system transaction with observable events.
        log.debug(f"  [block {block.height}] system tx: {exc}")

    return audit


# ---------------------------------------------------------------------------
# Optional: demonstrate get_transaction_result_by_index
# ---------------------------------------------------------------------------

async def show_result_by_index(client, block) -> None:
    """
    Bonus: fetch a single transaction result by its position within the block.
    Useful for targeted re-inspection without fetching all results.
    """
    result = await client.get_transaction_result_by_index(
        block_id=block.id, index=0
    )
    log.info(
        f"  get_transaction_result_by_index(index=0) → "
        f"status={result.status}  events={len(result.events)}"
    )


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def print_block_report(audit: BlockAudit, custody_addresses: set[str]) -> None:
    log.info(
        f"Block {audit.height}  ({audit.block_id[:12]}…)  "
        f"txs={audit.num_transactions}  sys_tx={audit.system_tx_scanned}  "
        f"deposits={len(audit.deposits)}  withdrawals={len(audit.withdrawals)}"
    )

    if not custody_addresses:
        return

    flagged = [d for d in audit.deposits if d.address in custody_addresses]
    if flagged:
        log.info(f"  *** {len(flagged)} deposit(s) to monitored custody address(es):")
        for d in flagged:
            log.info(
                f"      tx={d.tx_id[:16]}…  "
                f"amount={d.amount} FLOW  →  {d.address}"
            )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def run_audit(
    num_blocks: int = 10,
    start_height: Optional[int] = None,
    custody_addresses: Optional[list[str]] = None,
) -> None:
    """
    Walk ``num_blocks`` consecutive sealed blocks starting at ``start_height``
    (or the most recent ``num_blocks`` blocks if not specified) and report
    FLOW token deposits and withdrawals.

    Parameters
    ----------
    num_blocks : int
        Number of consecutive sealed blocks to scan.
    start_height : int, optional
        Block height to begin from. Defaults to latest_sealed − num_blocks + 1.
    custody_addresses : list[str], optional
        Custody addresses to flag in the log output.
    """
    watched: set[str] = set(custody_addresses or [])

    async with flow_client(host=MAINNET_HOST, port=MAINNET_PORT) as client:

        # Resolve the scan range
        latest = await client.get_latest_block(is_sealed=True)
        log.info(f"Latest sealed block: height={latest.height}  id={latest.id.hex()}")

        if start_height is None:
            start_height = max(0, latest.height - num_blocks + 1)
        end_height = start_height + num_blocks - 1
        log.info(f"Scanning blocks {start_height} → {end_height}")
        log.info("─" * 70)

        totals = {"deposits": 0, "withdrawals": 0, "flagged": 0}

        for height in range(start_height, end_height + 1):
            block = await client.get_block_by_height(height=height)
            audit = await audit_block(client, block)
            print_block_report(audit, watched)

            totals["deposits"] += len(audit.deposits)
            totals["withdrawals"] += len(audit.withdrawals)
            if watched:
                totals["flagged"] += sum(
                    1 for d in audit.deposits if d.address in watched
                )

            # Demonstrate get_transaction_result_by_index on the first block
            # that has at least one user transaction.
            if height == start_height and audit.num_transactions > 0:
                await show_result_by_index(client, block)

        log.info("─" * 70)
        log.info(f"Blocks scanned       : {num_blocks}")
        log.info(f"Total deposits       : {totals['deposits']}")
        log.info(f"Total withdrawals    : {totals['withdrawals']}")
        if watched:
            log.info(f"Flagged deposits     : {totals['flagged']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Audit FLOW token transfers using the flow-py-sdk bulk block APIs"
    )
    parser.add_argument(
        "--blocks",
        type=int,
        default=10,
        help="Number of consecutive sealed blocks to scan (default: 10)",
    )
    parser.add_argument(
        "--start-height",
        type=int,
        default=None,
        help="Starting block height (default: latest − blocks + 1)",
    )
    parser.add_argument(
        "--custody-address",
        dest="custody_addresses",
        action="append",
        default=[],
        metavar="ADDRESS",
        help="Custody address to flag in output (repeat for multiple addresses)",
    )
    args = parser.parse_args()

    asyncio.run(
        run_audit(
            num_blocks=args.blocks,
            start_height=args.start_height,
            custody_addresses=args.custody_addresses,
        )
    )
