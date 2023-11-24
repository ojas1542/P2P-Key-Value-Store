import asyncio
import threading
import time
import traceback
from typing import Coroutine
import flask
import aiohttp

from flask import request, Flask, jsonify
from flask_pydantic import validate as route_serde
from pydantic import StrictStr
from werkzeug.exceptions import HTTPException

from kvs.exception import ServerException, UpstreamDownException, ViewUninitializedException, TimeoutException, ValTooLargeException
from kvs.util import deasyncify_endpoint, hash_key, integer_ceiling_divide
from kvs.view import ViewData, ViewManager
from kvs.base_model import Model
from kvs.models import DataRequestBody, InternalNodeUpdate, PutSingleRequestBody, DataResponse, RequestBody, CausalMetadata, ModelResponse, Shard, View
from kvs.vc import VectorClock
from kvs.kv import KVSStateSnapshot
from pydantic import StrictStr, StrictInt


app = Flask(__name__)


app.config.update(
    FLASK_PYDANTIC_VALIDATION_ERROR_RAISE=True, 
)

view_manager = ViewManager()

@app.before_request
def before_request():
    app.logger.debug(f'before request: {request=} {request.data=}')

@app.after_request
def after_request(response: flask.Response):
    app.logger.debug(f'after request: {response=} {response.data=}')
    return response

# async def request_broadcast_single(*, view_data: ViewData, session: aiohttp.ClientSession, replica: str) -> None:
#     updates = view_data.request_queue.pop_all(replica)

async def request_broadcast_single(*, replica: str, view_data: ViewData, session: aiohttp.ClientSession) -> tuple[int, int]:
    updates = view_data.request_queue.pop_all(replica)
    num_tried = len(updates)
    try:
        if len(updates) == 0:
            return 0, 0
        res = await session.post(f'http://{replica}/kvs/internal/update', json=InternalUpdateRequestBody(updates=updates).dict(by_alias=True), timeout=aiohttp.ClientTimeout(total=1))
        num_succeeded = len(updates)
        if res.status != 200:
            view_data.request_queue.enqueue_all(replica, updates)
            num_succeeded = 0
    except:
        view_data.request_queue.enqueue_all(replica, updates)
        num_succeeded = 0
    return num_tried, num_succeeded


async def request_broadcast() -> tuple[int, int]:
    #iterate over request queues
    view_data = view_manager._get_cur_view_data()
    if view_data is None:
        return 0, 0

    total_tried, total_succeeded = 0, 0
    async with aiohttp.ClientSession() as session:
        for num_tried, num_succeeded in await asyncio.gather(*[
            request_broadcast_single(replica=replica, session=session, view_data=view_data)
            for shard in view_data.view.shards
            for replica in shard.nodes
        ]):
            total_tried += num_tried
            total_succeeded += num_succeeded
    return num_tried, num_succeeded

async def request_broadcast_task():
    broadcast_interval = 1  # in seconds
    while True:
        start_time = time.perf_counter()
        try:
            num_tried, num_succeeded = await request_broadcast()
            if num_tried > 0:
                app.logger.debug(f'broadcast tried {num_tried}, succeeded {num_succeeded}')
        except:
            traceback.print_exc()
        end_time = time.perf_counter()
        delta_time = end_time - start_time
        if delta_time > broadcast_interval:
            app.logger.warning(f'request broadcast took {delta_time:.2f} seconds')
        elif delta_time >= 0.01:
            app.logger.debug(f'request broadcast took {delta_time:.2f} seconds')
        if delta_time < broadcast_interval:
            await asyncio.sleep(broadcast_interval - delta_time)

def request_broadcast_thread_func():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(request_broadcast_task())
request_broadcast_thread = threading.Thread(target=request_broadcast_thread_func, daemon=True)
request_broadcast_thread.start()


async def request_proxy_single(replica,key,body):
    async with aiohttp.ClientSession() as session:
        res = await session.get(f"http://{replica}/kvs/data/{key}", json=body.dict(by_alias=True), timeout=aiohttp.ClientTimeout(total=1))
        return res
    

async def request_proxy(replicas,key,body):
    tasks = [request_proxy_single(replica,key,body) for replica in replicas]
    done,pending = await asyncio.wait([asyncio.create_task(t) for t in tasks], return_when=asyncio.FIRST_COMPLETED)
    res = await done.pop()
    return res



@app.errorhandler(Exception)
@app.errorhandler(HTTPException)
def handle_exception(e: Exception):
    if not isinstance(e, ViewUninitializedException):
        app.logger.exception(e)
    if isinstance(e, ServerException):
        return e.to_response()
    return jsonify({'error': 'bad request'}), 400


class ViewPutRequestBody(RequestBody):
    nodes: list[StrictStr]
    num_shards: StrictInt



class InternalViewUpdateRequestBody(RequestBody):
    view_num: StrictInt
    view: View
    nodes_to_tell: list[StrictStr] | None
    """(ignored if full_reshard is True)"""
    full_reshard: bool


@app.route('/kvs/admin/view', methods=['PUT'])
@route_serde(response_by_alias=True)
@deasyncify_endpoint
async def view_put(body: ViewPutRequestBody):
    num_shards = body.num_shards

    # get the old view (if any, default it to an empty view to simplify the logic)
    existing_view_data = view_manager._get_cur_view_data()
    if existing_view_data is not None:
        existing_view = existing_view_data.view
        new_view_num = existing_view_data.view_num + 1
    else:
        existing_view = View(__root__=[])
        new_view_num = 0

    # set of all the nodes in the old view. will not change.
    existing_view_nodes: frozenset[str] = frozenset(node for shard in existing_view.shards for node in shard.nodes)
    # set of all the nodes that will be in the new view. will not change.
    new_view_nodes: frozenset[str] = frozenset(body.nodes)
    # set of nodes that were in the old view but are not in the new view. will not change.
    removed_nodes: frozenset[str] = existing_view_nodes - new_view_nodes

    # free (unassigned) nodes in the new view. will be added to as we make necessary changes, then
    #  nodes will be taken from it so we know which nodes need to be assigned a shard later on in
    #  this function.
    free_nodes: set[str] = set(new_view_nodes - existing_view_nodes)

    # create new view, based on the old view, which we'll modify as needed
    new_view = existing_view.copy(deep=True)

    # True  = number of shards has changed, need to move everything
    # False = same number of shards, so just need to update the new/moved nodes
    # (will be set by one of the upcoming conditionals)
    full_migrate: bool

    # same number of shards, so just need to re-balance them
    if len(existing_view.shards) == num_shards:
        full_migrate = False
        # remove no-longer-used nodes from shards
        for shard in new_view.shards:
            nodes_to_remove = set(shard.nodes) & removed_nodes
            for node in nodes_to_remove:
                shard.nodes.remove(node)
    # different number of shards, everything will need to be moved around anyways so we'll just
    #  start from a clean slate
    else:
        full_migrate = True
        new_view.shards.clear()
        for i in range(num_shards):
            new_view.shards.append(Shard(shard_id=f'{i}', nodes=[]))
        for node in body.nodes:
            free_nodes.add(node)

    # set of valid shard sizes that will still result in us having balanced shards.
    valid_shard_sizes: set[int] = {
        len(new_view_nodes) // num_shards,
        integer_ceiling_divide(len(new_view_nodes), num_shards),
    }
    # first, make sure all free nodes are assigned to a shard
    while len(free_nodes) > 0:
        node_to_add = free_nodes.pop()
        smallest_shard = min(new_view.shards, key=lambda shard: len(shard.nodes))
        smallest_shard.nodes.append(node_to_add)
    # next, move shards until all nodes are balanced
    while any((len(shard.nodes) not in valid_shard_sizes) for shard in new_view.shards):
        # move one node from the largest remaining shard to the smallest remaining shard
        smallest_shard = min(new_view.shards, key=lambda shard: len(shard.nodes))
        largest_shard = max(new_view.shards, key=lambda shard: len(shard.nodes))
        # pick a node to remove from largest_shard, preferring nodes that:
        #   - are not us
        candidate_node = max(largest_shard.nodes, key=lambda node: (node != app.config['ADDRESS'],))
        largest_shard.nodes.remove(candidate_node)
        smallest_shard.nodes.append(candidate_node)

    # now that all the moves are finalized, we can figure out all the updates we need to send out

    my_shard_num: int
    for shard in new_view.shards:
        if app.config['ADDRESS'] in shard.nodes:
            my_shard_num = shard.shard_id_int
    # (if we don't end up finding the shard containing ourself then something's gone terribly wrong elsewhere, so probably ok to not check that here)

    # send everything out
    async with aiohttp.ClientSession() as session:
        delete_coros: list['aiohttp.client._RequestContextManager'] = []
        new_view_coros: list['aiohttp.client._RequestContextManager'] = []
        local_update_coros: list[Coroutine[None, None, None]] = []

        # TODO: retry failed requests?
        # first, tell all the removed nodes they've been removed.
        for replica in removed_nodes:
            delete_coros.append(session.delete("http://"+replica+"/kvs/admin/view", timeout=aiohttp.ClientTimeout(total=1)))

        if full_migrate:
            for shard in new_view.shards:
                for node in shard.nodes:
                    update = InternalViewUpdateRequestBody(
                        view_num=new_view_num,
                        view=new_view,
                        full_reshard=True,
                        nodes_to_tell=None,
                    )
                    if node == app.config['ADDRESS']:
                        local_update_coros.append(kvs_reshard_local(update))
                    else:
                        new_view_coros.append(session.post(f'http://{node}/kvs/internal/reshard', json=update.dict(by_alias=True)))
        else:
            for new_shard in new_view.shards:
                old_shard = existing_view.get_shard_by_id(new_shard.shard_id)
                new_shard_nodes = frozenset(new_shard.nodes)
                old_shard_nodes = frozenset(old_shard.nodes)
                nodes_already_in_shard = new_shard_nodes & old_shard_nodes
                nodes_added_to_shard = new_shard_nodes - old_shard_nodes
                for node in nodes_already_in_shard:
                    update = InternalViewUpdateRequestBody(
                        view_num=new_view_num,
                        view=new_view,
                        nodes_to_tell=list(nodes_added_to_shard - {node}),
                        full_reshard=False,
                    )
                    if node == app.config['ADDRESS']:
                        local_update_coros.append(kvs_reshard_local(update))
                    else:
                        new_view_coros.append(session.post(f'http://{node}/kvs/internal/reshard', json=update.dict(by_alias=True)))
                for node in nodes_added_to_shard:
                    update = InternalViewUpdateRequestBody(
                        view_num=new_view_num,
                        view=new_view,
                        nodes_to_tell=None,
                        full_reshard=False,
                    )
                    if node == app.config['ADDRESS']:
                        local_update_coros.append(kvs_reshard_local(update))
                    else:
                        new_view_coros.append(session.post(f'http://{node}/kvs/internal/reshard', json=update.dict(by_alias=True)))

        delete_outcomes, new_view_outcomes, local_update_outcomes = await asyncio.gather(
            asyncio.gather(*delete_coros, return_exceptions=True),
            asyncio.gather(*new_view_coros, return_exceptions=True),
            asyncio.gather(*local_update_coros, return_exceptions=True),
        )
        # TODO: should probably make sure all the new_view_outcomes succeeded

    return '', 200


class ViewGetResponse(Model):
    view: View

@app.route('/kvs/admin/view', methods=['GET'])
@route_serde(response_by_alias=True)
def view_get() -> ModelResponse[ViewGetResponse]:
    view_data = view_manager._get_cur_view_data()
    if view_data is None:
        return ViewGetResponse(view=View(__root__=[]))
    else:
        return ViewGetResponse(view=view_data.view)


@app.route('/kvs/admin/view', methods=['DELETE'])
@view_manager.require_init
def view_delete():
    view_manager.clear()
    return '', 200


class GetSingleResponse(DataResponse):
    val: StrictStr

class GetAllResponse(DataResponse):
    shard_id: StrictStr
    count: int
    keys: list[StrictStr]
    

@app.route('/kvs/data/<string:key>', methods=['PUT'])
@route_serde(response_by_alias=True)
@view_manager.require_init
@deasyncify_endpoint
async def kvs_put(key: str, body: PutSingleRequestBody, view_data: ViewData) -> ModelResponse[DataResponse]:
    #TODO: URLs have to be less than 2048 characters, which limits the keys; no further limit is needed on key lengths. 
    #If a value is larger than 8MB, the key-value store should reject the write and respond with status code 400 and JSON: {"error": "val too large"}.

    if(len((body.val).encode('utf-8')) > 8000000):
        raise ValTooLargeException()


    #generate 32bit hash for key
    key_shard = hash_key(key=key, modulo=view_data.num_shards)

    #if a key's hash mod num_shards doesn't equal the current node's shard id proxy to appropriate node
    if key_shard != view_data.my_shard_num:
        for shard in view_data.view.shards:
            if shard.shard_id_int == key_shard:
                t_end = time.time() + 20
                got500 = False
                #keep trying to proxy request to nodes in target shard until
                #one of them responds.
                proxy_targets: list['aiohttp.client._RequestContextManager'] = []
    

                async with aiohttp.ClientSession() as session:
                    while time.time() < t_end:
                        for replica in shard.nodes:
                            proxy_targets.append(session.put(f"http://{replica}/kvs/data/{key}", json=body.dict(by_alias=True), timeout=aiohttp.ClientTimeout(total=1)))
                
                        done = await asyncio.gather(*proxy_targets,return_exceptions=True)

                        for r in done:
                            try:
                                if r.status == 200 or r.status == 201:
                                    res_body = await r.text()
                                    return res_body,r.status
                                if r.status_code == 500:
                                    got500 = True
                            except:
                                pass

                    #if all but one server was reachable and that server couldn't resolve it's dependencies
                    #return 500
                    if got500:
                        raise TimeoutException()

                    raise UpstreamDownException(shard)

    arrival_time = time.time_ns()

    metadata = body.causal_metadata
    replicaID = app.config['ADDRESS']


    if metadata.vc is None:
        metadata.vc = view_data.local_vc
    if metadata.view_num is None:
        metadata.view_num = view_data.view_num
    elif metadata.view_num < view_data.view_num:
        metadata.vc = view_data.local_vc
        metadata.view_num = view_data.view_num
    view_data.local_vc = view_data.local_vc.merge_with_respect_to_shard(metadata.vc, view_data.my_shard)
    
    view_data.local_vc = view_data.local_vc.with_increment(replicaID)

    response_vc = metadata.vc.merge_with_respect_to_shard(view_data.local_vc, view_data.my_shard)


    if view_data.kv.has(key):
        status = 200
    else:
        status = 201

    res = DataResponse(causal_metadata = CausalMetadata(vc=response_vc, view_num=view_data.view_num))

    update = InternalNodeUpdate(key=key, val=body.val, vc=response_vc, view_num=view_data.view_num, origin_address=replicaID, timestamp=arrival_time, client_address=f'{request.remote_addr}')
    view_data.buffer_queue.append(update)
    view_data.commit_all_updates_possible()

    #add request to send queues for approprite replicas
    for replica in view_data.my_shard.nodes:
        if replica != app.config['ADDRESS']:
            view_data.request_queue.enqueue(replica,update)

    return res, status




@app.route('/kvs/data/<string:key>', methods=['GET'])
@route_serde(response_by_alias=True)
@view_manager.require_init
@deasyncify_endpoint
async def kvs_get(key: str, body: DataRequestBody, view_data: ViewData) -> ModelResponse[DataResponse]:
    metadata = body.causal_metadata

    #generate 32bit hash for key
    key_shard = hash_key(key=key, modulo=view_data.num_shards)

    #if a key's hash mod num_shards doesn't equal the current node's shard id proxy to appropriate node
    if key_shard != view_data.my_shard_num:
        for shard in view_data.view.shards:
            if shard.shard_id_int == key_shard:
                t_end = time.time() + 20
                got500 = False
                #keep trying to proxy request to nodes in target shard until
                #one of them responds.
                proxy_targets: list['aiohttp.client._RequestContextManager'] = []
    

                async with aiohttp.ClientSession() as session:
                    while time.time() < t_end:
                        for replica in shard.nodes:
                            proxy_targets.append(session.get(f"http://{replica}/kvs/data/{key}", json=body.dict(by_alias=True), timeout=aiohttp.ClientTimeout(total=1)))
                
                        done = await asyncio.gather(*proxy_targets,return_exceptions=True)

                        for r in done:
                            try:
                                if r.status == 200 or r.status == 404:
                                    res_body = await r.text()
                                    return res_body,r.status
                                if r.status_code == 500:
                                    got500 = True
                            except:
                                pass

                    #if all but one server was reachable and that server couldn't resolve it's dependencies
                    #return 500
                    if got500:
                        raise TimeoutException()

                    raise UpstreamDownException(shard)

    if metadata.vc is None:
        metadata.vc = view_data.local_vc
    if metadata.view_num is None:
        metadata.view_num = view_data.view_num
    elif metadata.view_num < view_data.view_num:
        metadata.vc = view_data.local_vc
        metadata.view_num = view_data.view_num
    view_data.local_vc = view_data.local_vc.merge_with_respect_to_shard(metadata.vc, view_data.my_shard)
    await view_data.wait_for_consistency_with(metadata.vc.subset_relevant_to_shard(view_data.my_shard), metadata.view_num)

    response_vc = metadata.vc.merge_with_respect_to_shard(view_data.local_vc, view_data.my_shard)
    val = view_data.kv.get(key)
    if val is None:
        return DataResponse(causal_metadata=CausalMetadata(vc=response_vc, view_num=view_data.view_num)), 404
    else:
        return GetSingleResponse(val=val, causal_metadata=CausalMetadata(vc=response_vc, view_num=view_data.view_num)), 200




@app.route('/kvs/data/<string:key>', methods=['DELETE'])
@route_serde(response_by_alias=True)
@view_manager.require_init
@deasyncify_endpoint
async def kvs_delete(key: str, body: DataRequestBody, view_data: ViewData):
    arrival_time = time.time_ns()

    #generate 32bit hash for key
    key_shard = hash_key(key=key, modulo=view_data.num_shards)

    #if a key's hash mod num_shards doesn't equal the current node's shard id proxy to appropriate node
    if key_shard != view_data.my_shard_num:
        for shard in view_data.view.shards:
            if shard.shard_id_int == key_shard:
                t_end = time.time() + 20
                got500 = False
                #keep trying to proxy request to nodes in target shard until
                #one of them responds.
                proxy_targets: list['aiohttp.client._RequestContextManager'] = []
    

                async with aiohttp.ClientSession() as session:
                    while time.time() < t_end:
                        for replica in shard.nodes:
                            proxy_targets.append(session.delete(f"http://{replica}/kvs/data/{key}", json=body.dict(by_alias=True), timeout=aiohttp.ClientTimeout(total=1)))
                
                        done = await asyncio.gather(*proxy_targets,return_exceptions=True)

                        for r in done:
                            try:
                                if r.status == 200 or r.status == 404:
                                    res_body = await r.text()
                                    return res_body,r.status
                                if r.status_code == 500:
                                    got500 = True
                            except:
                                pass

                    #if all but one server was reachable and that server couldn't resolve it's dependencies
                    #return 500
                    if got500:
                        raise TimeoutException()

                    raise UpstreamDownException(shard)

    metadata = body.causal_metadata
    replicaID = app.config['ADDRESS']

    if metadata.vc is None:
        metadata.vc = view_data.local_vc
    if metadata.view_num is None:
        metadata.view_num = view_data.view_num
    elif metadata.view_num < view_data.view_num:
        metadata.vc = view_data.local_vc
        metadata.view_num = view_data.view_num
    view_data.local_vc = view_data.local_vc.merge_with_respect_to_shard(metadata.vc, view_data.my_shard)
    view_data.local_vc = view_data.local_vc.with_increment(replicaID)

    if view_data.kv.has(key):
        status = 200
    else:
        status = 404
    response_vc = metadata.vc.merge_with_respect_to_shard(view_data.local_vc, view_data.my_shard)
    res = DataResponse(causal_metadata=CausalMetadata(vc=response_vc, view_num=view_data.view_num))

    update = InternalNodeUpdate(key=key, val=None, vc=response_vc, view_num=view_data.view_num, origin_address=replicaID, timestamp=arrival_time, client_address=f'{request.remote_addr}')
    view_data.buffer_queue.append(update)
    view_data.commit_all_updates_possible()

    #add request to send queues for approprite replicas
    for shard in view_data.view.shards:
        if shard.shard_id_int == key_shard:
            for replica in shard.nodes:
                if replica != app.config['ADDRESS']:
                    view_data.request_queue.enqueue(replica,update)

    return res, status


# TODO - should `/kvs/data/` go to `/kvs/data`, or should it be a per-key operation with an empty string for the key?
@app.route('/kvs/data', methods=['GET'])
@app.route('/kvs/data/', methods=['GET'])
@route_serde(response_by_alias=True)
@view_manager.require_init
@deasyncify_endpoint
async def kvs_get_all(body: DataRequestBody, view_data: ViewData) -> ModelResponse[GetAllResponse]:
    metadata = body.causal_metadata

    if metadata.vc is None:
        metadata.vc = view_data.local_vc
    if metadata.view_num is None:
        metadata.view_num = view_data.view_num
    elif metadata.view_num < view_data.view_num:
        metadata.vc = view_data.local_vc
        metadata.view_num = view_data.view_num
    view_data.local_vc = view_data.local_vc.merge_with_respect_to_shard(metadata.vc, view_data.my_shard)
    await view_data.wait_for_consistency_with(metadata.vc.subset_relevant_to_shard(view_data.my_shard), metadata.view_num)

    keys = view_data.kv.list_keys()

    response_vc = metadata.vc.merge_with_respect_to_shard(view_data.local_vc, view_data.my_shard)
    res = GetAllResponse(shard_id=str(view_data.my_shard_num), count=len(keys), keys=keys, causal_metadata=CausalMetadata(vc=response_vc, view_num=view_data.view_num))
    return res,200


class InternalUpdateRequestBody(Model):
    updates: list[InternalNodeUpdate]


def should_enqueue_update(*, view_data: ViewData, update: InternalNodeUpdate) -> bool:
    if update.origin_address == app.config['ADDRESS']:
        return False
    # app.logger.debug(f'should_enqueue_update({update=})    =    {update.vc=} not in {view_data.seen_vcs=}    =    {update.vc not in view_data.seen_vcs}')
    # if update.vc < max_committed:
    #     return False
    return update.vc not in view_data.seen_vcs

@app.route('/kvs/internal/update', methods=['POST'])
@route_serde(response_by_alias=True)
@view_manager.require_init
def kvs_internal_update(body: InternalUpdateRequestBody, view_data: ViewData) -> tuple[str, int]:
    to_queue = []
    for update in body.updates:
        if should_enqueue_update(view_data=view_data, update=update):
            to_queue.append(update)
        if update.vc not in view_data.seen_vcs:
            view_data.seen_vcs.append(update.vc)
    view_data.buffer_queue.extend(to_queue)
    view_data.commit_all_updates_possible()
    return '', 200

class InternalViewChangeDataTransfer(RequestBody):
    view_num: int
    view: View  # need to also attach the view to these so that we can create the view data if we haven't already
    data: KVSStateSnapshot

@app.route('/kvs/internal/reshard_data', methods=['POST'])
@route_serde(response_by_alias=True)
def kvs_reshard_data_update(body: InternalViewChangeDataTransfer):
    kvs_reshard_data_update_local(body)
    return '', 200

def kvs_reshard_data_update_local(body: InternalViewChangeDataTransfer):
    # create the data for the new view if it didn't already exist
    # TODO: should maybe redesign this a bit so we don't need to find the shard num again every time
    my_shard_num: int | None = None
    for shard in body.view.shards:
        if app.config['ADDRESS'] in shard.nodes:
            my_shard_num = shard.shard_id_int
    assert my_shard_num is not None
    new_view_data = view_manager.create_data_for_new_view(
        view_num=body.view_num,
        view=body.view,
        my_shard_num=my_shard_num,
    )
    # actually put the updates we got into the kvs
    new_view_data.kv.put_snapshot(body.data)

@app.route('/kvs/internal/reshard', methods=['POST'])
@route_serde(response_by_alias=True)
@deasyncify_endpoint
async def kvs_reshard(body: InternalViewUpdateRequestBody):
    await kvs_reshard_local(body)
    return '', 200

async def kvs_reshard_local(body: InternalViewUpdateRequestBody) -> None:
    # create our new view data
    my_shard_num: int | None = None
    for shard in body.view.shards:
        if app.config['ADDRESS'] in shard.nodes:
            my_shard_num = shard.shard_id_int
    assert my_shard_num is not None
    new_view_data = view_manager.create_data_for_new_view(
        view_num=body.view_num,
        view=body.view,
        my_shard_num=my_shard_num,
    )
    prev_view_data = view_manager.get_view_data_before(new_view_data)
    async with aiohttp.ClientSession() as session:
        request_cors: list['aiohttp.client._RequestContextManager'] = []
        if prev_view_data is not None:
            if body.full_reshard:
                snapshots = prev_view_data.kv.make_reshard_snapshots(new_view_data)
                for shard in new_view_data.view.shards:
                    shard_snapshot = snapshots[shard.shard_id_int]
                    for node in shard.nodes:
                        update = InternalViewChangeDataTransfer(
                            view=new_view_data.view,
                            view_num=new_view_data.view_num,
                            data=shard_snapshot,
                        )
                        if node == app.config['ADDRESS']:
                            kvs_reshard_data_update_local(update)
                        else:
                            request_cors.append(session.post(f'http://{node}/kvs/internal/reshard_data', json=update.dict(by_alias=True)))
            else:
                snapshot = prev_view_data.kv.make_snapshot()
                new_view_data.kv.put_snapshot(snapshot)
                if body.nodes_to_tell is not None:
                    for node in body.nodes_to_tell:
                        update = InternalViewChangeDataTransfer(
                            view=new_view_data.view,
                            view_num=new_view_data.view_num,
                            data=snapshot,
                        )
                        if node == app.config['ADDRESS']:
                            kvs_reshard_data_update_local(update)
                        else:
                            request_cors.append(session.post(f'http://{node}/kvs/internal/reshard_data', json=update.dict(by_alias=True)))
        request_outcomes = await asyncio.gather(*request_cors, return_exceptions=True)
        # TODO did they succeed?


# TODO hey are we actually using this
@app.route('/kvs/internal/shard', methods=['GET'])
@route_serde(response_by_alias=True)
@view_manager.require_init
def kvs_get_shard(view_data: ViewData) -> ModelResponse[Shard]:
    return view_data.my_shard


@app.route('/asg3tester/alivetest', methods=['GET'])
def _tester_alivetest():
    return jsonify({'alive': True})
