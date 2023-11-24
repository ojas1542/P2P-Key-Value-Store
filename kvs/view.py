import asyncio
from functools import wraps
import inspect
from threading import Lock
import time
from typing import Callable, TypeVar, cast, get_type_hints
from kvs.exception import TimeoutException
from kvs.kv import KeyValueStore
from kvs.models import InternalNodeUpdate, Shard, View
from kvs.requestQueue import RequestQueue
from kvs.util import wraps_and_consumes_props
from kvs.vc import VectorClock

T_Func = TypeVar('T_Func', bound=Callable)

class ViewData:
    """
    all of the data associated with a given view.
    """
    view: View
    view_num: int
    local_vc: VectorClock
    buffer_queue: list[InternalNodeUpdate]
    max_committed: VectorClock
    seen_vcs: list[VectorClock]
    my_shard_num: int
    request_queue: RequestQueue
    kv: KeyValueStore

    def __init__(self, *, view: View, view_num: int, my_shard_num: int, kv: KeyValueStore) -> None:
        self.view = view
        self.view_num = view_num
        self.my_shard_num = my_shard_num
        self.local_vc = VectorClock.empty()
        self.buffer_queue = []
        self.max_committed = VectorClock.empty()
        self.seen_vcs = []
        self.request_queue = RequestQueue()
        self.kv = kv

    # TODO: probably only need to compute this once, switch to functools.cached_property?
    @property
    def my_shard(self) -> Shard:
        for shard in self.view.shards:
            if shard.shard_id_int == self.my_shard_num:
                return shard
        assert False  # should be unreachable

    @property
    def num_shards(self) -> int:
        return len(self.view.shards)

    async def wait_for_consistency_with(self, vc: VectorClock, view_num: int):
        start_time = time.time()
        # TODO probably better to use a threading event or smth rather than just sleep+poll
        while True:
            elapsed = time.time() - start_time
            from kvs.app import app; app.logger.debug(f'waiting for consistency with {vc=}. ({view_num=} {self.view_num=}, {elapsed=:0.2f}) -- [[[ {self.local_vc=} ]]] -- || {self.buffer_queue=} || {self.max_committed=}')
            if view_num < self.view_num:
                return
            if vc < self.max_committed or vc == self.max_committed:
                return
            if elapsed > 20:
                raise TimeoutException()
            await asyncio.sleep(0.25)

    def _is_commit_candidate(self, update: InternalNodeUpdate) -> bool:
        return self.max_committed[update.origin_address] + 1 == update.vc[update.origin_address]

    def _commit_update(self, update: InternalNodeUpdate):
        # app.logger.debug(f'committing {update=}')
        self.max_committed = self.max_committed.merge_single(update.vc, update.origin_address)
        self.local_vc = VectorClock.merge(update.vc, self.local_vc)
        self.kv.put(update.key, update)

    def commit_all_updates_possible(self):
        while (update := next(filter(self._is_commit_candidate, self.buffer_queue), None)) is not None:
            self.buffer_queue.remove(update)
            self._commit_update(update)

class ViewManager:
    _views: dict[int, ViewData]
    __lock: Lock

    def __init__(self) -> None:
        self._views = dict()
        self.__lock = Lock()

    def clear(self) -> None:
        self._views.clear()

    def create_data_for_new_view(self, *, view_num: int, view: View, my_shard_num: int) -> ViewData:
        """
        create the data for a new view. if a view with that number already exists, does nothing (but will still return the existing view data).
          view_num     - the number of the new view
          view         - the view itself
          my_shard_num - the id of the shard in the new view containing ourself
        returns the `ViewData` for the new (or existing) view data
        """
        # acquiring a lock for the duration of this just in case it gets called again before the first call gets to finish
        with self.__lock:
            existing_view_data = self._views.get(view_num)
            if existing_view_data is not None:
                return existing_view_data
            old_view = self._views.get(view_num - 1, None)
            if old_view is not None:
                new_kv = old_view.kv.copy()
            else:
                new_kv = KeyValueStore()
            new_view = ViewData(
                view=view,
                view_num=view_num,
                my_shard_num=my_shard_num,
                kv=new_kv,
            )
            self._views[view_num] = new_view
            return new_view

    def get_view_data_before(self, view_data: ViewData) -> ViewData | None:
        return self._views.get(view_data.view_num - 1, None)

    def _get_cur_view_data(self) -> ViewData | None:
        newest_view = max(self._views.items(), key=lambda kv: kv[0], default=None)
        if newest_view is None:
            return None
        return newest_view[1]

    def require_init(self, func: T_Func) -> T_Func:
        # (importing locally to avoid circular import)
        from kvs.exception import ViewUninitializedException
        func_sig = inspect.signature(func)
        consumed_props = []
        pass_view_data = 'view_data' in func_sig.parameters
        if pass_view_data:
            consumed_props.append('view_data')
            hints = get_type_hints(func)
            assert hints.get('view_data', None) == ViewData
        @wraps_and_consumes_props(func, *consumed_props)
        def wrapper(*args, **kwargs):
            cur_view_data = self._get_cur_view_data()
            if cur_view_data is None:
                raise ViewUninitializedException()
            bonus_kwargs: dict = {}
            if pass_view_data:
                bonus_kwargs['view_data'] = cur_view_data
            return func(*args, **kwargs, **bonus_kwargs)
        return cast(T_Func, wrapper)

