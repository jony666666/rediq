import json
import pickle

from abc import ABC, abstractstaticmethod
from typing import Literal, Any

class Serializer(ABC):

    @abstractstaticmethod
    def dumps(inp: Any) -> bytes: ...

    @abstractstaticmethod
    def loads(inp: bytes) -> Any: ...

class JSONSerializer(Serializer):

    @staticmethod
    def dumps(inp: Any) -> bytes:
        return json.dumps(inp).encode('utf-8')

    @staticmethod
    def loads(inp: bytes) -> Any:
        return json.loads(inp.decode('utf-8'))

class UJSONSerializer(Serializer):

    @staticmethod
    def dumps(inp: Any) -> bytes:
        import ujson
        return ujson.dumps(inp).encode('utf-8')

    @staticmethod
    def loads(inp: bytes) -> Any:
        import ujson
        return ujson.loads(inp.decode('utf-8'))

class PickleSerializer(Serializer):

    @staticmethod
    def dumps(inp: Any):
        return pickle.dumps(inp, protocol=pickle.HIGHEST_PROTOCOL)
    
    @staticmethod
    def loads(inp: bytes):
        return pickle.loads(inp)
    
def get_serializer(name: Literal['json', 'ujson', 'pickle']) -> Serializer:

    if name == 'json':
        return JSONSerializer
    if name == 'ujson':
        return UJSONSerializer
    if name == 'pickle':
        return PickleSerializer
    
    raise ValueError(f'no serializer named "{name}"')
