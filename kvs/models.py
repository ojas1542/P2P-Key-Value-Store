import enum
from typing import TypeVar, Union
from pydantic import Extra, Field, StrictStr, StrictInt
from typing import Dict

from kvs.base_model import Model, ImmutableModel
from kvs.vc import VectorClock

class CausalMetadata(Model):
    view_num: StrictInt | None
    vc: VectorClock | None

class RequestBody(Model):
    # not 100% sure subclassing superclass's config class is necessary here, but it can't hurt so might as well just in case
    class Config(Model.Config):
        # " extra keys (and values) in a JSON object are not considered malformed
        extra = Extra.ignore

class DataRequestBody(RequestBody):
    causal_metadata: CausalMetadata = Field(alias='causal-metadata')

class PutSingleRequestBody(DataRequestBody):
    val: StrictStr

class DataResponse(Model):
    causal_metadata: CausalMetadata = Field(alias='causal-metadata')

class InternalNodeUpdate(ImmutableModel):
    client_address: str
    timestamp: int
    """arrival time - should be from `time.time_ns()`"""
    key: StrictStr
    val: StrictStr | None
    """will be None for deletes, str for writes"""
    view_num: StrictInt
    vc: VectorClock
    origin_address: StrictStr
    """address of the node this update originated from (i.e. the node that the client directly talked to)"""

T_model = TypeVar('T_model', bound=Model)
ModelResponse = Union[tuple[T_model, int], T_model]

class Shard(Model):
    shard_id: StrictStr
    nodes: list[StrictStr]

    @property
    def shard_id_int(self) -> int:
        return int(self.shard_id)

class View(Model):
    __root__: list[Shard]

    @property
    def shards(self) -> list[Shard]:
        return self.__root__

    def get_shard_by_id(self, shard_id: str) -> Shard:
        """get the shard corresponding to the given id. if no such shard found, raises KeyError."""
        for shard in self.shards:
            if shard.shard_id == shard_id:
                return shard
        raise KeyError(shard_id)
