import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from flow_py_sdk.client import entities
from flow_py_sdk.client.client import AccessAPI
from flow_py_sdk.proto.flow.access import AccessApiStub, GetSystemTransactionResultRequest
from flow_py_sdk.proto.flow.access import TransactionResultResponse as ProtoTransactionResultResponse
from flow_py_sdk.proto.flow import entities as proto_entities


class TestGetSystemTransactionResult(unittest.IsolatedAsyncioTestCase):
    async def test_returns_transaction_result_response(self):
        block_id = bytes(32)
        proto_response = ProtoTransactionResultResponse(
            status=proto_entities.TransactionStatus.SEALED,
            status_code=0,
            error_message="",
            events=[],
        )
        mock_channel = MagicMock()
        client = AccessAPI(channel=mock_channel)

        with patch.object(
            AccessApiStub,
            "get_system_transaction_result",
            new=AsyncMock(return_value=proto_response),
        ):
            result = await client.get_system_transaction_result(block_id=block_id)

        self.assertIsInstance(result, entities.TransactionResultResponse)
        self.assertEqual(proto_entities.TransactionStatus.SEALED, result.status)
        self.assertEqual(0, result.status_code)
        self.assertEqual([], result.events)

    async def test_passes_correct_request_to_stub(self):
        block_id = bytes.fromhex("ab" * 32)
        proto_response = ProtoTransactionResultResponse(
            status=proto_entities.TransactionStatus.PENDING,
            status_code=0,
            error_message="",
            events=[],
        )
        mock_channel = MagicMock()
        client = AccessAPI(channel=mock_channel)

        with patch.object(
            AccessApiStub,
            "get_system_transaction_result",
            new=AsyncMock(return_value=proto_response),
        ) as mock_stub:
            await client.get_system_transaction_result(block_id=block_id)
            mock_stub.assert_called_once_with(
                GetSystemTransactionResultRequest(block_id=block_id)
            )

    async def test_error_response_preserved(self):
        block_id = bytes(32)
        proto_response = ProtoTransactionResultResponse(
            status=proto_entities.TransactionStatus.EXECUTED,
            status_code=1,
            error_message="cadence runtime error",
            events=[],
        )
        mock_channel = MagicMock()
        client = AccessAPI(channel=mock_channel)

        with patch.object(
            AccessApiStub,
            "get_system_transaction_result",
            new=AsyncMock(return_value=proto_response),
        ):
            result = await client.get_system_transaction_result(block_id=block_id)

        self.assertEqual(1, result.status_code)
        self.assertEqual("cadence runtime error", result.error_message)
