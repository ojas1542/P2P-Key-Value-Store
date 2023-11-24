from typing import TYPE_CHECKING
from pydantic import StrictInt, StrictStr
from collections.abc import Mapping

from kvs.base_model import ImmutableModel

# (avoid circular import)
if TYPE_CHECKING:
    from kvs.models import Shard

class VectorClock(ImmutableModel):
    # using Mapping rather than dict here since Mapping is immutable (or at least pretends to be
    #  during type checking)
    __root__: Mapping[StrictStr, StrictInt]

    def _key_set(self) -> frozenset[str]:
        return frozenset(self.__root__.keys())

    def __getitem__(self, key: str) -> int:
        return self.__root__.get(key, 0)

    @staticmethod
    def merge(a: 'VectorClock', b: 'VectorClock') -> 'VectorClock':
        """merge two vector clocks"""
        return VectorClock(__root__={k: max(a[k], b[k]) for k in a._key_set() | b._key_set()})

    def merge_single(self, other: 'VectorClock', which: str) -> 'VectorClock':
        """merge this vector clock with only one component of another vector clock."""
        return VectorClock(__root__=self.__root__ | {which: max(self[which], other[which])})

    def merge_with_respect_to_shard(self, other: 'VectorClock', shard: 'Shard') -> 'VectorClock':
        return VectorClock.merge(self, other.subset_relevant_to_shard(shard))
 
    @staticmethod
    def empty() -> 'VectorClock':
        """create a vector clock with nothing in it"""
        return VectorClock(__root__={})

    def with_increment(self, key: str) -> 'VectorClock':
        """return a new vector clock (i.e. DOES NOT MUTATE IN PLACE) with the value associated with the given key incremented by one"""
        return VectorClock(__root__=self.__root__ | {key: self[key] + 1})

    def __lt__(self, rhs: 'VectorClock') -> bool:
        keys = self._key_set() | rhs._key_set()
        return (
            all(self[key] <= rhs[key] for key in keys)
            and any(self[key] < rhs[key] for key in keys)
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, VectorClock):
            return False
        keys = self._key_set() | other._key_set()
        return all(self[key] == other[key] for key in keys)

    def is_empty(self) -> bool:
        return all(val < 1 for val in self.__root__.values())

    def subset_relevant_to_shard(self, shard: 'Shard') -> 'VectorClock':
        return VectorClock(__root__={
            address: val
            for address, val in self.__root__.items()
            if address in shard.nodes
        })
