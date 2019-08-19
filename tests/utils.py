from os.path import join
from pathlib import Path

path = Path(__file__).resolve()
ROOT = path.parents[1]
TEST = join(ROOT, 'tests')

TCP_PUBLISHER_ADDRESS = "127.0.0.1"
TCP_COLLECTOR_ADDRESS = "127.0.0.1"

TCP_PORT = "5556"
TCP_PROTOCOL = "tcp://"

TCP_PUBLISHER_URL_SOCKET = TCP_PROTOCOL + TCP_PUBLISHER_ADDRESS + ":" + TCP_PORT
TCP_COLLECTOR_URL_SOCKET = TCP_PROTOCOL + TCP_COLLECTOR_ADDRESS + ":" + TCP_PORT
