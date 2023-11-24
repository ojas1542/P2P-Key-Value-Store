import argparse
import logging
import os
from kvs.app import app

def _split_url_port_address(s: str) -> tuple[str, str]:
    ip, sep, port = s.rpartition(':')
    assert sep == ':'
    return ip, port

if 'ADDRESS' in os.environ:
    ip, port = _split_url_port_address(os.environ['ADDRESS'])
else:
    ip, port = '0.0.0.0', '8080'

parser = argparse.ArgumentParser()
parser.add_argument('--host', type=str, default=ip)
parser.add_argument('--port', type=str, default=port)
parser.add_argument('--debug', action='store_true')
args = parser.parse_args()
app.logger.setLevel(logging.DEBUG)
app.config.update(ADDRESS=f'{args.host}:{args.port}')
app.run(host=args.host, port=args.port, debug=args.debug)
