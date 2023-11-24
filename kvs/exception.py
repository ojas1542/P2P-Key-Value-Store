from abc import ABC, abstractmethod

from flask import Response, jsonify

from kvs.models import Shard

class ServerException(Exception, ABC):
    @property
    @abstractmethod
    def message(self) -> str: ...

    @property
    @abstractmethod
    def code(self) -> int: ...

    def to_json(self) -> dict:
        return {'error': self.message}

    def to_response(self) -> tuple[Response, int]:
        return jsonify(self.to_json()), self.code

# " You should respond to a malformed request with a status code of 400 and a JSON body of {"error": "bad request"}
class MalformedRequestException(ServerException):
    message = 'bad request'
    code = 400

# " If the inconsistency is not resolved within 20 seconds, the replica should respond with
#   status code 500 and JSON body: {"error": "timed out while waiting for depended updates"}
class TimeoutException(ServerException):
    message = 'timed out while waiting for depended updates'
    code = 500

# " When a node is in the uninitialized state, it should respond to all requests, other than PUT and
#   GET requests to /kvs/admin/view (defined below), with status code 418 and body {"error": "uninitialized"}
class ViewUninitializedException(ServerException):
    message = 'uninitialized'
    code = 418

# " If a value is larger than 8MB, the key-value store should reject the write and respond with
#   status code 400 and JSON: {"error": "val too large"}
class ValTooLargeException(ServerException):
    message = 'val too large'
    code = 400

# " If a node cannot proxy a request to any of the nodes that host a shard within 20 seconds, it
#   should respond with status code 503 and the following response:
#   {
#     "error": "upstream down",
#     "upstream": {"shard_id": "<shard_id>", "nodes": [<nodes that host shard shard_id>]}
#   }
# TODO: make sure serialized form of this is formatted correctly
class UpstreamDownException(ServerException):
    message = 'upstream down'
    code = 503

    upstream: Shard

    def __init__(self, upstream: Shard) -> None:
        super().__init__()
        self.upstream = upstream

    def to_json(self) -> dict:
        return super().to_json() | {'upstream': self.upstream.dict(by_alias=True)}
