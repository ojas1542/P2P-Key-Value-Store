from typing import TYPE_CHECKING

from kvs.base_model import ImmutableModel
from kvs.models import InternalNodeUpdate
from kvs.util import hash_key

# (to avoid a circular import)
if TYPE_CHECKING:
    from kvs.view import ViewData

# all things considered not sure if this is the best choice for how to design this part, but it's good enough for now
class KVSStateSnapshot(ImmutableModel):
    __root__: dict[str, InternalNodeUpdate]

def resolve_conflict(existing: InternalNodeUpdate | None, incoming: InternalNodeUpdate) -> InternalNodeUpdate:
    if existing is None:
        return incoming
    if existing.view_num < incoming.view_num:
        return incoming
    if incoming.view_num < existing.view_num:
        return existing
    if existing.vc < incoming.vc:
        return incoming
    if incoming.vc < existing.vc:
        return existing
    if (existing.timestamp, existing.client_address, existing.origin_address) > (incoming.timestamp, incoming.client_address, incoming.origin_address):
        return existing
    else:
        return incoming

class KeyValueStore:
    _kv_dict: dict[str, InternalNodeUpdate]

    def __init__(self) -> None:
        self._kv_dict = {}

    def make_snapshot(self) -> KVSStateSnapshot:
        return KVSStateSnapshot(__root__=self._kv_dict)

    def make_reshard_snapshots(self, new_view_data: 'ViewData') -> dict[int, KVSStateSnapshot]:
        snapshot_datas: dict[int, dict[str, InternalNodeUpdate]] = {
            shard.shard_id_int: {}
            for shard in new_view_data.view.shards
        }
        for k, v in self._kv_dict.items():
            key_shard = hash_key(key=k, modulo=new_view_data.num_shards)
            snapshot_datas[key_shard][k] = v
        return {
            shard_id_int: KVSStateSnapshot(__root__=snapshot_data)
            for shard_id_int, snapshot_data in snapshot_datas.items()
        }

    def put_snapshot(self, snapshot: KVSStateSnapshot):
        for key, update in snapshot.__root__.items():
            self.put(key, update)

    def has(self, key: str) -> bool:
        entry = self._kv_dict.get(key, None)
        if entry is None:
            return False
        else:
            return entry.val is not None

    def put(self, key: str, entry: InternalNodeUpdate) -> None:
        """
        set value in the store, unless conflict resolution says that entry loses to the current entry
        """
        self._kv_dict[key] = resolve_conflict(self._kv_dict.get(key, None), entry)

    def get(self, key: str) -> str | None:
        """
        get the current local value associated with the key (or None if key not here).
        """
        entry = self._kv_dict.get(key, None)
        if entry is None:
            return None
        return entry.val

    def list_keys(self) -> list[str]:
        keys = []
        for key, entry in self._kv_dict.items():
            if entry.val is not None:
                keys.append(key)
        return keys

    def copy(self) -> 'KeyValueStore':
        new_kv = KeyValueStore()
        new_kv._kv_dict = self._kv_dict.copy()
        return new_kv
