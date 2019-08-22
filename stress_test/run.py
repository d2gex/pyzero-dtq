import sys

from pyzero_dtq.collector import Collector
from stress_test import AppIO

if __name__ == "__main__":

    url = sys.argv[1]
    publisher_url = sys.argv[2]
    max_processes = sys.argv[3]

    collector = Collector(url, publisher_url, max_processes)
    collector.app = AppIO
    collector.run()
