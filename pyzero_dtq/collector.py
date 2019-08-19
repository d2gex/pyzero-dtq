import multiprocessing

from pymulproc import factory, mpq_protocol
from producer_sink.sink import Sink
from pyzero_dtq.iprocess import IProcess
from pyzero_dtq.worker import Worker
from pyzero_dtq.publisher import Publisher
from pyzero_dtq.application import Application


class Collector(IProcess):

    def __init__(self, url, publisher_url, max_workers):
        super().__init__()
        self.url = url
        self.publisher_url = publisher_url
        self.pub_identity = f"publisher_{self.publisher_url.split(':')[2]}"
        self.max_workers = max_workers
        self.workers = []
        self.workers_running = 0
        self._app = None
        self.sink = None
        self.publisher = None

        # Create queues through which we will talks with both workers and the publisher
        self.task_queue_factory = factory.QueueCommunication()
        self.result_queue_factory = factory.QueueCommunication()
        self.task_queue = self.task_queue_factory.parent()
        self.result_queue = self.result_queue_factory.parent()

        # Start our Sink peer to listen to incoming tasks
        self.sink = Sink(url=url, identity='Collector')

    @property
    def app(self):
        return self._app

    @app.setter
    def app(self, application):
        if not issubclass(application, Application):
            raise ValueError(f"'application' argument should be a subclass of {Application}")
        self._app = application

    def start_worker(self, loops=True):
        '''Start a worker process
        '''

        def run_worker(app, task_queue_factory, result_queue_factory):
            worker = Worker()
            worker.app = app()
            worker.task_queue = task_queue_factory.child()
            worker.result_queue = result_queue_factory.child()
            worker.run(loops)

        child_process = multiprocessing.Process(target=run_worker,
                                                args=(self.app,
                                                      self.task_queue_factory,
                                                      self.result_queue_factory))
        child_process.start()
        return child_process

    def start_publisher(self, loops=True):

        def run_publisher(publisher_url, identity, result_queue_factory):
            publisher = Publisher(publisher_url, identity)
            publisher.result_queue = result_queue_factory.child()
            publisher.run(loops)

        child_process = multiprocessing.Process(target=run_publisher,
                                                args=(self.publisher_url,
                                                      self.pub_identity,
                                                      self.result_queue_factory))
        child_process.start()
        return child_process

    def kill_workers(self):
        for _ in self.workers:
            self.task_queue.send(mpq_protocol.REQ_DIE)

    def kill_publisher(self):
        if self.publisher:
            self.result_queue.send(mpq_protocol.REQ_DIE)

    def clean(self):

        # Release sink resource
        if self.sink:
            self.sink.clean()

        # Kill all running workers if running
        if self.workers:
            self.kill_workers()
            for child in self.workers:
                child.join()

        # Kill the publisher if running
        if self.publisher:
            self.kill_publisher()
            self.publisher.join()

    def run(self, loops=True):
        stop = False

        try:
            while not stop and loops:
                task = self.sink.run()
                if task:
                    self.task_queue.send(mpq_protocol.REQ_DO, data=task[-1])
                    if self.workers_running < self.max_workers:
                        child_process = self.start_worker()
                        self.workers.append(child_process)
                        self.workers_running += 1

                if not stop and not isinstance(loops, bool):
                    loops -= 1
        except KeyboardInterrupt:
            pass
        finally:
            self.clean()
