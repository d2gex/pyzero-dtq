import multiprocessing
import time

from collections import defaultdict
from pymulproc import mpq_protocol, factory
from producer_sink.producer import Producer
from pubsub_zmq.subscriber import Subscriber
from pyzero_dtq.collector import Collector
from tests import utils as test_utils, stubs


class TestSystem:


    @staticmethod
    def start_producer(url, identity, topic, tasks):

        def run_producer():
            '''Producers will run one time in a fire-and-forget policy releasing the socket at the end of it.
            '''
            producer = Producer(url=url, identity=identity)
            for task in tasks:
                producer.run(
                    [mpq_protocol.REQ_DO, {'topic': topic, 'info': task}]
                )
            # keep alive for a while until the message is actually sent before releasing the socket
            time.sleep(0.1)
            producer.clean()
        child_process = multiprocessing.Process(target=run_producer)
        child_process.start()
        return child_process

    @staticmethod
    def start_subscriber(url, identity, topic, loops, comm):

        def run_subscriber():
            '''Subscribers will run for a few loops each waiting for the distributed task queue to send an back the
            results and then release the socket at the end
            '''
            subscriber = Subscriber(topic, url=url, identity=identity)
            for _ in range(loops):
                data = subscriber.run()
                if data:
                    comm.send(mpq_protocol.REQ_DO, data=data)
            subscriber.clean()

        child_process = multiprocessing.Process(target=run_subscriber)
        child_process.start()
        return child_process

    @staticmethod
    def start_collector(url, publisher_url, app, loops, max_workers):

        def run_collector():
            '''Start the collector process which in turn will run start the publisher explicitly and workers as
            tasks are being poured in
            '''
            # Instantiate collector
            collector = Collector(url=url,
                                  publisher_url=publisher_url,
                                  max_workers=max_workers)
            # Add App that workers need to run
            collector.app = app
            # Start publisher and keep track of it
            collector.publisher = collector.start_publisher(loops)
            # Run the collector
            collector.run(loops)
            assert collector.workers_running == max_workers
            assert len(collector.workers) == max_workers
            assert collector.publisher

        child_process = multiprocessing.Process(target=run_collector)
        child_process.start()
        return child_process

    def test_functionality(self):
        '''Test the distributed task queue as a whole
        '''

        queue_factory = factory.QueueCommunication()
        parent = queue_factory.parent()

        # (1) Run Collector for 500 * 10(polling-in) = 3 seconds
        max_workers = 10
        child_collector = self.start_collector(url=test_utils.TCP_COLLECTOR_URL_SOCKET,
                                               publisher_url=test_utils.TCP_PUBLISHER_URL_SOCKET,
                                               app=stubs.AppStub,
                                               loops=500,
                                               max_workers=max_workers)
        assert child_collector.pid

        # (2) Run Subscribers for 500 * 10 (polling-in) = 5 seconds
        child_subscribers = []
        for offset in range(max_workers):
            child = self.start_subscriber(url=test_utils.TCP_PUBLISHER_URL_SOCKET,
                                          identity=f'subscriber_{offset}',
                                          topic=offset,
                                          loops=500,
                                          comm=queue_factory.child())
            child_subscribers.append(child)

        time.sleep(0.5)  # ---> Give time to collector and subscribers to be ready

        # (3) Run Producers
        # --> 10 producers sends 10 tasks each to the collector => 100 tasks
        child_producers = []
        num_tasks = 10
        tasks = [f'task_{x}' for x in range(num_tasks)]
        for offset in range(max_workers):
            child = self.start_producer(url=test_utils.TCP_COLLECTOR_URL_SOCKET,
                                        identity=f'producer_{offset}',
                                        topic=offset,
                                        tasks=tasks)
            child_producers.append(child)

        # Wait for producers
        for child in child_producers:
            child.join()

        # Wait for subscribers
        for child in child_subscribers:
            child.join()

        # Wait for collector child
        child_collector.join()

        # Assert the results
        expected_results = max_workers * num_tasks
        returned_results = 0
        results = defaultdict(set)
        while not parent.queue_empty():
            returned_result = parent.receive()
            topic, data = returned_result[-1]
            results[topic].add(data)
            returned_results += 1
        assert returned_results == expected_results
        # Ensure all topics received are the ones expected
        assert all(_topic in results for _topic in range(num_tasks))
        # Ensure all topics received does have num_tasks results
        assert all(len(results[_topic]) == num_tasks for _topic in results)
