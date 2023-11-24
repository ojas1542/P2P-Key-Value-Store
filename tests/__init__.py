from asyncio import sleep, gather
import asyncio
import contextlib
import itertools
import logging
import time
from typing import TYPE_CHECKING, Any, AsyncIterator, Iterable, Literal, Type, TypeVar, cast, overload
import unittest
import asg3tester
from asg3tester import start_client, start_node
if TYPE_CHECKING:
    import aiohttp.client

logger = logging.getLogger(__name__)
# logger.setLevel()
logging.basicConfig(level=logging.INFO)

@contextlib.asynccontextmanager
async def start_n_nodes(n: int, /) -> AsyncIterator[list[asg3tester.NodeApi]]:
    async with contextlib.AsyncExitStack() as stack:
        nodes = []
        for _ in range(n):
            nodes.append(await stack.enter_async_context(start_node()))
        yield nodes

T = TypeVar('T')

class TestAssignment3(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        await asg3tester.setup(asg3tester.Config(image_name='kvs:3.0'))

    async def asyncTearDown(self) -> None:
        await asg3tester.cleanup()

    @overload
    async def _assertResponseLooksLike(self, response: 'aiohttp.client.ClientResponse', *, status: int | None = None, has_keys: set[str] | None = None, has_vals: dict[str, Any] | None = None, return_body: Literal[False] = False, msg: str | None = None) -> None: ...
    @overload
    async def _assertResponseLooksLike(self, response: 'aiohttp.client.ClientResponse', *, status: int | None = None, has_keys: set[str] | None = None, has_vals: dict[str, Any] | None = None, return_body: Literal[True], msg: str | None = None) -> dict[str, Any]: ...
    async def _assertResponseLooksLike(
            self,
            response: 'aiohttp.client.ClientResponse',
            *,
            status: int | None = None,
            has_keys: set[str] | None = None,
            has_vals: dict[str, Any] | None = None,
            return_body: bool = False,
            msg: str | None = None,
      ) -> dict[str, Any] | None:
        """
        assert that the response:
            - if `status` specified, has that status
            - if `has_keys` specified or `has_vals` specified or `return_body`=True, parses body as json and asserts that it's a dict
            - if `return_body`=True, returns parsed body
            - if `has_keys` specified, body has all those keys
            - if `has_vals` specified, body has all those keys, with those values
        returns parsed response body, or None if wasn't valid json (or if was json but wasn't a dict at top level)
        """
        msg_extra = f'(response data:{await response.read()!r})'
        if msg is not None:
            final_msg = f'{msg} {msg_extra}'
        else:
            final_msg = msg_extra
        if status is not None:
            self.assertEqual(response.status, status, msg=final_msg)
        if has_keys is not None or has_vals is not None or return_body:
            body = await response.json(content_type=None)
            self.assertIsInstance(body, dict, msg=final_msg)
        if has_keys is not None:
            for key in has_keys:
                self.assertIn(key, body, msg=final_msg)
        if has_vals is not None:
            for key, val in has_vals.items():
                self.assertIn(key, body, msg=final_msg)
                self.assertEqual(val, body[key], msg=final_msg)
        if return_body:
            # (need to cast here since the unittest instance asserts aren't properly recognized by mypy)
            return cast(dict[str, Any], body)
        return None

    def _assertHasKeyOfType(self, d: dict, key: str, typ: Type[T]) -> T:
        self.assertIn(key, d)
        val = d[key]
        self.assertIsInstance(val, typ)
        return cast(T, val)

    def _assertIsIterableWithType(self, it: Iterable[T], typ: Type[T]) -> list[T]:
        ret = list[T]()
        for a in it:
            self.assertIsInstance(a, typ)
            ret.append(a)
        return ret

    def assertBetween(self, low, t, high, *, low_inclusive: bool=True, high_inclusive: bool=True):
        if low_inclusive:
            self.assertLessEqual(low, t)
        else:
            self.assertLess(low, t)
        if high_inclusive:
            self.assertGreaterEqual(high, t)
        else:
            self.assertGreater(high, t)

    async def test_get_1node(self):
        async with start_node() as a, start_client() as client:
            await a.view_put_asg4(1,[a.address])
            await client.data_single_put(a, key='k', val='v')
            r = await client.data_single_get(a, key='k')
            await self._assertResponseLooksLike(r, status=200, has_vals={'val': 'v'}, has_keys={'causal-metadata'})

    async def test_delete_then_get_1node(self):
        async with start_node() as a, start_client() as client:
            await a.view_put_asg4(1,[a.address])
            await client.data_single_put(a, key='k', val='v')
            await client.data_single_delete(a, key='k')
            r = await client.data_single_get(a, key='k')
            await self._assertResponseLooksLike(r, status=404, has_keys={'causal-metadata'})

    async def test_get_2nodes(self):
        async with start_node() as a, start_node() as b, start_client() as client:
            await a.view_put_asg4(1,[a.address, b.address])
            await client.data_single_put(a, key='k', val='v')
            r = await client.data_single_get(b, key='k')
            await self._assertResponseLooksLike(r, status=200, has_vals={'val': 'v'}, has_keys={'causal-metadata'})

    async def test_simple_partition_get_timeout(self):
        async with start_node() as a, start_node() as b, start_client() as client:
            await a.view_put_asg4(1,[a.address, b.address])
            await a.create_partition(b)
            await client.data_single_put(a, key='k', val='v')
            # TODO make sure it takes ~20 seconds, error if much more than that
            r = await client.data_single_get(b, key='k')
            await self._assertResponseLooksLike(r, status=500, has_vals={'error': 'timed out while waiting for depended updates'})

    async def test_simple_partition_get_healed_eventually(self):
        async with start_node() as a, start_node() as b, start_client() as client:
            await a.view_put_asg4(1,[a.address, b.address])
            await a.create_partition(b)
            await client.data_single_put(a, key='k', val='v')
            async def sleep_then_heal():
                await sleep(8)
                await a.heal_partition(b)
            r, _ = await asyncio.gather(client.data_single_get(b, key='k'), sleep_then_heal())
            await self._assertResponseLooksLike(r, status=200, has_vals={'val': 'v'}, has_keys={'causal-metadata'})


    async def test_concurrent_write_reconciliation_with_healed_partition(self):
        async with start_node() as a, start_node() as b, start_client() as client1,start_client() as client2:
            await a.view_put_asg4(1,[a.address, b.address])
            await a.create_partition(b)
            await client1.data_single_put(a, key='k', val='v1')
            await client2.data_single_put(b, key='k', val='v2')
            async def sleep_then_heal():
                await sleep(8)
                await a.heal_partition(b)
            r_a, _ = await asyncio.gather(client2.data_single_get(a, key='k'), sleep_then_heal())
            r_b = await client1.data_single_get(b, key='k')
            body_a, body_b = [await self._assertResponseLooksLike(r, has_keys={'val'}, return_body=True) for r in (r_a, r_b)]
            val_a, val_b = [self._assertHasKeyOfType(body, 'val', str) for body in (body_a, body_b)]
            self.assertEqual(val_a, val_b, msg='''replicas didn't agree''')


    async def test_view_get(self):
        async with start_node() as a, start_node() as b:
            view = ['10.10.0.2:8080', '10.10.0.3:8080']
            await a.view_put_asg4(1,view)
            r = await a.view_get()
            await self._assertResponseLooksLike(r, status=200, has_keys={'view'})

    async def test_uninitialized(self):
        async with start_node() as a:
            async def foo():
                yield 'view DELETE', a.view_delete()
                yield 'data single PUT', a.data_single_put(key='k', val='v', causal_metadata={})
                yield 'data single GET', a.data_single_get(key='k', causal_metadata={})
                yield 'data single DELETE', a.data_single_delete(key='k', causal_metadata={})
                yield 'data all GET', a.data_all_get(causal_metadata={})
            async for testname, request in foo():
                with self.subTest(testname):
                    r = await request
                    await self._assertResponseLooksLike(r, status=418, has_vals={'error': 'uninitialized'}, msg='"When a node is in the uninitialized state, it should respond to all requests, other than PUT and GET requests to /kvs/admin/view (defined below), with status code 418 and body {"error": "uninitialized"}"')
            with self.subTest('view GET'):
                r = await a.view_get()
                await self._assertResponseLooksLike(r, status=200, has_vals={'view': []})

    async def test_eventual_consistency_01(self):
        async with start_node() as n1, start_node() as n2, start_node() as n3, start_client() as c1, start_client() as c2:
            await n1.view_put_asg4(1,[n1.address, n2.address, n3.address])
            await n2.create_partition(n1)
            await n2.create_partition(n3)
            await gather(
                c1.data_single_put(n1, 'k', 'v1'),
                c2.data_single_put(n3, 'k', 'v2'),
            )
            await sleep(2)
            await n2.heal_partition(n1)
            await n2.heal_partition(n3)
            await sleep(20)
            overall_val = None
            for r in await gather(*[n.data_single_get('k', causal_metadata={}) for n in (n1, n2, n3)]):
                await self._assertResponseLooksLike(r, status=200, has_keys={'causal-metadata', 'val'})
                val = (await r.json())['val']
                if overall_val is None:
                    self.assertIn(val, {'v1', 'v2'})
                    overall_val = val
                else:
                    self.assertEqual(val, overall_val)

    async def test_causally_consistent_reads_01(self):
        async with start_node() as a, start_node() as b, start_client() as c1, start_client() as c2, start_client() as c3:
            await a.view_put_asg4(1,[a.address, b.address])
            await a.create_partition(b)
            r = await c1.data_single_put(a, 'k1', 'v1_1')
            await self._assertResponseLooksLike(r, status=201, has_keys={'causal-metadata'})
            r = await c2.data_single_get(a, 'k1')
            await self._assertResponseLooksLike(r, status=200, has_vals={'val': 'v1_1'}, has_keys={'causal-metadata'})
            r = await c2.data_single_put(b, 'k2', 'v2_1')
            await self._assertResponseLooksLike(r, status=201, has_keys={'causal-metadata'})
            r = await c3.data_single_get(b, 'k1')
            await self._assertResponseLooksLike(r, status=500, has_vals={'error': 'timed out while waiting for depended updates'})

    async def test_read_timeout_duration(self):
        async with start_node() as a, start_node() as b, start_client() as client:
            await a.view_put_asg4(1,[a.address, b.address])
            await a.create_partition(b)
            await client.data_single_put(a, 'k', 'v')
            start_time = time.time()
            r = await client.data_single_get(b, 'k')
            end_time = time.time()
            await self._assertResponseLooksLike(r, status=500, has_vals={'error': 'timed out while waiting for depended updates'})
            self.assertAlmostEqual(end_time - start_time, 20, delta=1)

    async def test_view_change_add_node(self):
        async with start_node() as a, start_node() as b, start_node() as c, start_client() as client, start_client() as client2:
            await a.view_put_asg4(1,[a.address, b.address])
            await client.data_single_put(a, 'k', 'v')
            await sleep(10)
            await a.kill()
            await b.view_put_asg4(1,[b.address, c.address])
            r = await client2.data_single_get(c, 'k')
            await self._assertResponseLooksLike(r, status=200, has_vals={'val': 'v'}, has_keys={'causal-metadata'})


    async def test_request_proxy_health_network(self):
        async with start_node() as a, start_node() as b, start_client() as client:
            await a.view_put_asg4(2,[a.address, b.address])
            await client.data_single_put(b, 'hello', 'world') 
            r = await client.data_single_get(b, 'hello')
            await self._assertResponseLooksLike(r, status=200, has_vals={'val': 'world'}, has_keys={'causal-metadata'})

    async def test_resharding(self):
        max_shards = 3
        max_nodes_per_shard = 3
        async with start_n_nodes(max_shards * max_nodes_per_shard) as nodes:
            a, *others = nodes
            for num_shards in range(1, max_shards + 1):
                for num_nodes in range(num_shards, max_shards * max_nodes_per_shard + 1):
                    with self.subTest(num_shards=num_shards, num_nodes=num_nodes):
                        await a.view_put_asg4(num_shards=num_shards, nodes=[a.address, *(node.address for node in others[:num_nodes - 1])])
                        r = await a.view_get()
                        body = await self._assertResponseLooksLike(r, status=200, has_keys={'view'}, return_body=True)
                        valid_shard_sizes = {
                            num_nodes // num_shards,
                            -(num_nodes // -num_shards),  # (integer ceiling division, from https://stackoverflow.com/a/17511341/8762161)
                        }
                        view = self._assertHasKeyOfType(body, 'view', list)
                        seen_shard_ids = set[str]()
                        for shard in self._assertIsIterableWithType(view, dict):
                            shard_id = self._assertHasKeyOfType(shard, 'shard_id', str)
                            self.assertLessEqual(len(shard_id), 40, msg='"The ID of each shard ... [must be] at most 40 characters long"')
                            self.assertNotIn(shard_id, seen_shard_ids, msg=f'duplicate shard id {shard_id!r}')
                            seen_shard_ids.add(shard_id)
                            shard_nodes = self._assertHasKeyOfType(shard, 'nodes', list)
                            self.assertIn(len(shard_nodes), valid_shard_sizes, f'shards not balanced! shard {shard_id!r}')

    async def test_even_key_distribution(self):
        num_shards = 4
        num_keys = 4096
        async with start_n_nodes(4) as nodes, start_client() as client:
            a, *others = nodes
            await a.view_put_asg4(num_shards=num_shards, nodes=[node.address for node in nodes])
            for i in range(num_keys):
                await self._assertResponseLooksLike(await client.data_single_put(a, key=f'key{i}', val=f'val{i}'), return_body=True)
            shard_responsedatas = [
                await self._assertResponseLooksLike(await client.data_all_get(node), has_keys={'shard_id', 'causal-metadata', 'count', 'keys'}, return_body=True)
                for node in nodes
            ]
            overall_total = 0
            min_valid_size = (num_keys / num_shards) * 0.9
            max_valid_size = (num_keys / num_shards) * 1.1
            for i, body in enumerate(shard_responsedatas):
                shard_size = self._assertHasKeyOfType(body, 'count', int)
                # check if it's balanced
                with self.subTest(f'shard #{i + 1} balanced?'):
                    logger.warning(f'shard #{i + 1} balance: {shard_size / num_keys * 100:.5f}%')
                    self.assertBetween(min_valid_size, shard_size, max_valid_size)

    # TODO add more schema tests

    async def test_data_across_reshard_same_numbers_different_nodes(self):
        async with start_n_nodes(8) as all_nodes, start_client() as client:
            a1, a2, b1, b2, c1, c2, d1, d2 = all_nodes
            first_view_nodes = [a1, a2, b1, b2, c1, c2]
            await a1.view_put_asg4(num_shards=3, nodes=[node.address for node in first_view_nodes])
            data = {f'key{i}': f'val{i}' for i in range(32)}
            for node, (k, v) in zip(itertools.cycle(first_view_nodes), data.items()):
                await client.data_single_put(node, key=k, val=v)
            await asyncio.sleep(10)
            second_view_nodes = [a2, b1, b2, c1, c2, d1]
            await a2.view_put_asg4(num_shards=3, nodes=[node.address for node in second_view_nodes])
            for node, (k, v) in zip(itertools.cycle(second_view_nodes), data.items()):
                print('a')
                r = await client.data_single_get(node, key=k)
                await self._assertResponseLooksLike(r, status=200, has_keys={'causal-metadata'}, has_vals={'val': v})

    async def test_data_across_reshard_different_shard_count(self):
        async with start_n_nodes(8) as all_nodes, start_client() as client:
            a1, a2, b1, b2, c1, c2, d1, d2 = all_nodes
            first_view_nodes = [a1, a2, b1, b2, c1, c2]
            await a1.view_put_asg4(num_shards=3, nodes=[node.address for node in first_view_nodes])
            data = {f'key{i}': f'val{i}' for i in range(32)}
            for node, (k, v) in zip(itertools.cycle(first_view_nodes), data.items()):
                await client.data_single_put(node, key=k, val=v)
            await asyncio.sleep(10)
            second_view_nodes = [a2, b1, b2, c1, c2, d1]
            await a2.view_put_asg4(num_shards=2, nodes=[node.address for node in second_view_nodes])
            for node, (k, v) in zip(itertools.cycle(second_view_nodes), data.items()):
                r = await client.data_single_get(node, key=k)
                await self._assertResponseLooksLike(r, status=200, has_keys={'causal-metadata'}, has_vals={'val': v})

    async def test_withshards_partition_get_timeout(self):
        async with start_n_nodes(6) as nodes, start_client() as client:
            a, b, c, d, e, f = nodes
            addr_to_node = {node.address: node for node in nodes}
            await a.view_put_asg4(2, [node.address for node in nodes])
            view_body = await self._assertResponseLooksLike(await a.view_get(), status=200, return_body=True)
            shards = self._assertIsIterableWithType(self._assertHasKeyOfType(view_body, 'view', list), dict)
            shard_1, shard_2 = shards
            for addr1, addr2 in itertools.product(shard_1['nodes'], shard_2['nodes']):
                await addr_to_node[addr1].create_partition(addr_to_node[addr2])
            await client.data_single_put(addr_to_node[shard_1['nodes'][0]], key='k', val='v')
            # TODO make sure it takes ~20 seconds, error if much more than that
            r = await client.data_single_get(addr_to_node[shard_2['nodes'][0]], key='k')
            body = await self._assertResponseLooksLike(r, status=503, has_vals={'error': 'upstream down'}, has_keys={'upstream'}, return_body=True)
            upstream = self._assertHasKeyOfType(body, 'upstream', dict)
            self._assertHasKeyOfType(upstream, 'shard_id', str)
            self._assertIsIterableWithType(self._assertHasKeyOfType(upstream, 'nodes', list), str)

    async def test_withshards_partition_get_healed_eventually(self):
        async with start_n_nodes(6) as nodes, start_client() as client:
            a, b, c, d, e, f = nodes
            addr_to_node = {node.address: node for node in nodes}
            await a.view_put_asg4(2, [node.address for node in nodes])
            view_body = await self._assertResponseLooksLike(await a.view_get(), status=200, return_body=True)
            shards = self._assertIsIterableWithType(self._assertHasKeyOfType(view_body, 'view', list), dict)
            shard_1, shard_2 = shards
            for addr1, addr2 in itertools.product(shard_1['nodes'], shard_2['nodes']):
                await addr_to_node[addr1].create_partition(addr_to_node[addr2])
            await client.data_single_put(addr_to_node[shard_1['nodes'][0]], key='k', val='v')
            async def sleep_then_heal():
                await sleep(8)
                for addr1, addr2 in itertools.product(shard_1['nodes'], shard_2['nodes']):
                    await addr_to_node[addr1].heal_partition(addr_to_node[addr2])
            r, _ = await asyncio.gather(client.data_single_get(addr_to_node[shard_2['nodes'][0]], key='k'), sleep_then_heal())
            await self._assertResponseLooksLike(r, status=200, has_vals={'val': 'v'}, has_keys={'causal-metadata'})


if __name__ == '__main__':
    logging.basicConfig()
    # logging.getLogger('asg3tester').setLevel(logging.DEBUG)
    unittest.main()

