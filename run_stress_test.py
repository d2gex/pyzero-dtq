import sys
import multiprocessing

from pyzero_dtq.collector import Collector
from pyzero_dtq.appio import AppIO

if __name__ == "__main__":

    multiprocessing.current_process().name = 'Hola'
    url = sys.argv[1]
    publisher_url = sys.argv[2]
    max_processes = sys.argv[3]

    collector = Collector(url, publisher_url, max_processes)
    collector.app = AppIO
    collector.run()
