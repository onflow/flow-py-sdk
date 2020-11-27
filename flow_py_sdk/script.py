import json
import logging
from typing import Optional

from flow_py_sdk.cadence.encode import CadenceJsonEncoder
from flow_py_sdk.cadence.types import Value

log = logging.getLogger(__name__)


class Script(object):
    def __init__(self, *, code: str, arguments: list[Value] = None) -> None:
        super().__init__()
        self.code: Optional[str] = code
        self.arguments: list[Value] = arguments if arguments else []

    def add_arguments(self, *args: Value) -> 'Script':
        self.arguments.extend(args)
        return self

    def encoded_arguments(self) -> list[bytes]:
        return [json.dumps(a, ensure_ascii=False, cls=CadenceJsonEncoder).encode('utf-8') for a in self.arguments]
