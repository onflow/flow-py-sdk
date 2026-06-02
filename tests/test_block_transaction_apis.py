import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from flow_py_sdk.client import entities
from flow_py_sdk.client.client import AccessAPI
from flow_py_sdk.proto.flow.access import (
    AccessApiStub,
    GetTransactionsByBlockIdRequest,
    TransactionResultsResponse as ProtoTransactionResultsResponse,
    TransactionsResponse as ProtoTransactionsResponse,
    TransactionResultResponse as ProtoTransactionResultResponse,
)
from flow_py_sdk.proto.flow import entities as proto_entities


class TestGetTransactionResultsByBlockId(unittest.IsolatedAsyncioTestCase):
    async def test_returns_list_of_transaction_result_responses(self):
        block_id = bytes(32)
        proto_response = ProtoTransactionResultsResponse(
            transaction_results=[
                ProtoTransactionResultResponse(
                    status=proto_entities.TransactionStatus.SEALED,
                    status_code=0,
                    error_message="",
                    events=[],
                ),
                ProtoTransactionResultResponse(
                    status=proto_entities.TransactionStatus.EXECUTED,
                    status_code=1,
                    error_message="revert",
                    events=[],
                ),
            ]
        )
        client = AccessAPI(channel=MagicMock())
        with patch.object(
            AccessApiStub,
            "get_transaction_results_by_block_id",
            new=AsyncMock(return_value=proto_response),
        ):
            results = await client.get_transaction_results_by_block_id(block_id=block_id)

        self.assertEqual(2, len(results))
        self.assertIsInstance(results[0], entities.TransactionResultResponse)
        self.assertEqual(proto_entities.TransactionStatus.SEALED, results[0].status)
        self.assertEqual(proto_entities.TransactionStatus.EXECUTED, results[1].status)
        self.assertEqual("revert", results[1].error_message)

    async def test_passes_correct_request_to_stub(self):
        block_id = bytes.fromhex("ab" * 32)
        proto_response = ProtoTransactionResultsResponse(transaction_results=[])
        client = AccessAPI(channel=MagicMock())
        with patch.object(
            AccessApiStub,
            "get_transaction_results_by_block_id",
            new=AsyncMock(return_value=proto_response),
        ) as mock_stub:
            await client.get_transaction_results_by_block_id(block_id=block_id)
            mock_stub.assert_called_once_with(
                GetTransactionsByBlockIdRequest(block_id=block_id)
            )

    async def test_empty_block_returns_empty_list(self):
        block_id = bytes(32)
        proto_response = ProtoTransactionResultsResponse(transaction_results=[])
        client = AccessAPI(channel=MagicMock())
        with patch.object(
            AccessApiStub,
            "get_transaction_results_by_block_id",
            new=AsyncMock(return_value=proto_response),
        ):
            results = await client.get_transaction_results_by_block_id(block_id=block_id)
        self.assertEqual([], results)


class TestGetTransactionsByBlockId(unittest.IsolatedAsyncioTestCase):
    async def test_returns_list_of_transactions(self):
        block_id = bytes(32)
        proto_tx = proto_entities.Transaction(
            script=b"transaction {}",
            payer=bytes(8),
        )
        proto_response = ProtoTransactionsResponse(transactions=[proto_tx])
        client = AccessAPI(channel=MagicMock())
        with patch.object(
            AccessApiStub,
            "get_transactions_by_block_id",
            new=AsyncMock(return_value=proto_response),
        ):
            results = await client.get_transactions_by_block_id(block_id=block_id)

        self.assertEqual(1, len(results))
        self.assertIsInstance(results[0], entities.Transaction)

    async def test_passes_correct_request_to_stub(self):
        block_id = bytes.fromhex("cd" * 32)
        proto_response = ProtoTransactionsResponse(transactions=[])
        client = AccessAPI(channel=MagicMock())
        with patch.object(
            AccessApiStub,
            "get_transactions_by_block_id",
            new=AsyncMock(return_value=proto_response),
        ) as mock_stub:
            await client.get_transactions_by_block_id(block_id=block_id)
            mock_stub.assert_called_once_with(
                GetTransactionsByBlockIdRequest(block_id=block_id)
            )
