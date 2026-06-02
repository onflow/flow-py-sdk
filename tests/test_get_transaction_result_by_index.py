import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from flow_py_sdk.client import entities
from flow_py_sdk.client.client import AccessAPI
from flow_py_sdk.proto.flow.access import AccessApiStub
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
