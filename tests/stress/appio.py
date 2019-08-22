import time

from collections import deque
from threading import Thread
from pyzero_dtq.application import Application


class AppIO(Application):
    def __init__(self):
        self.results = deque()

    def do_something(self, response, wait):
        '''Sleep and add the receiving response to a queue
        '''
        time.sleep(wait)
        self.results.append(response)

    def accept(self, criteria):
        return True

    def run(self, task):
        '''Creates a thread for each piece of information to be processed that will sleep for a given time and return
        a result. The sum of all those overlapped times is the processing time this class will take and the sume of all
        results returned.
        '''

        data = task['info']
        threads = []
        for item in data:
            response, wait = item
            threads.append(Thread(target=self.do_something, args=(response, wait)))
            threads[-1].start()

        for thread in threads:
            thread.join()

        return {
            'topic': task['topic'],
            'info': sum(self.results)
        }
