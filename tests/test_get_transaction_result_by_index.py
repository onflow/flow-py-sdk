import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from flow_py_sdk.client import entities
from flow_py_sdk.client.client import AccessAPI
from flow_py_sdk.proto.flow.access import AccessApiStub, GetTransactionByIndexRequest
from flow_py_sdk.proto.flow.access import TransactionResultResponse as ProtoTransactionResultResponse
from flow_py_sdk.proto.flow import entities as proto_entities


class TestTransactionResultFromProto(unittest.TestCase):
    def test_from_proto_with_default_id(self):
        proto_response = ProtoTransactionResultResponse(
            status=proto_entities.TransactionStatus.SEALED,
            status_code=0,
            error_message="",
            events=[],
        )
        result = entities.TransactionResultResponse.from_proto(proto_response)
        self.assertEqual(b"", result.id)
        self.assertEqual(proto_entities.TransactionStatus.SEALED, result.status)
        self.assertEqual(0, result.status_code)
        self.assertEqual("", result.error_message)
        self.assertEqual([], result.events)

    def test_from_proto_with_explicit_id_still_works(self):
        tx_id = bytes.fromhex("ab" * 32)
        proto_response = ProtoTransactionResultResponse(
            status=proto_entities.TransactionStatus.PENDING,
            status_code=1,
            error_message="execution reverted",
            events=[],
        )
        result = entities.TransactionResultResponse.from_proto(proto_response, id=tx_id)
        self.assertEqual(tx_id, result.id)
        self.assertEqual(proto_entities.TransactionStatus.PENDING, result.status)
        self.assertEqual(1, result.status_code)
        self.assertEqual("execution reverted", result.error_message)


class TestGetTransactionResultByIndex(unittest.IsolatedAsyncioTestCase):
    async def test_returns_transaction_result_response(self):
        block_id = bytes(32)
        index = 2
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
            "get_transaction_result_by_index",
            new=AsyncMock(return_value=proto_response),
        ):
            result = await client.get_transaction_result_by_index(
                block_id=block_id, index=index
            )

        self.assertIsInstance(result, entities.TransactionResultResponse)
        self.assertEqual(proto_entities.TransactionStatus.SEALED, result.status)
        self.assertEqual(b"", result.id)
        self.assertEqual([], result.events)

    async def test_passes_correct_parameters_to_stub(self):
        block_id = bytes.fromhex("ab" * 32)
        index = 5
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
            "get_transaction_result_by_index",
            new=AsyncMock(return_value=proto_response),
        ) as mock_stub:
            await client.get_transaction_result_by_index(
                block_id=block_id, index=index
            )
            mock_stub.assert_called_once_with(GetTransactionByIndexRequest(block_id=block_id, index=index))

    async def test_error_response_preserved(self):
        block_id = bytes(32)
        index = 0
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
            "get_transaction_result_by_index",
            new=AsyncMock(return_value=proto_response),
        ):
            result = await client.get_transaction_result_by_index(
                block_id=block_id, index=index
            )

        self.assertEqual(1, result.status_code)
        self.assertEqual("cadence runtime error", result.error_message)
