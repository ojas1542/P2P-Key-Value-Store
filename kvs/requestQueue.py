from collections import defaultdict
from kvs.models import InternalNodeUpdate, PutSingleRequestBody,DataRequestBody

# req_type,key,body  ||  str,str,DataRequestBody

class RequestQueue():
    _queue_dict: defaultdict[str, list[InternalNodeUpdate]]

    def __init__(self) -> None:
        self._queue_dict = defaultdict(list)

    def enqueue(self, replica: str, update: InternalNodeUpdate) -> None:
        self._queue_dict[replica].append(update)

    def __getitem__(self, replica: str) -> list[InternalNodeUpdate]:
        return self._queue_dict[replica]

    def pop_all(self, replica: str) -> list[InternalNodeUpdate]:
        ret = self._queue_dict[replica]
        self._queue_dict[replica] = []
        return ret

    def clear(self):
        self._queue_dict.clear()

    def enqueue_all(self, replica: str, updates: list[InternalNodeUpdate]) -> None:
        self._queue_dict[replica].extend(updates)
