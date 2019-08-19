import pytest
import multiprocessing
import time

from pymulproc import mpq_protocol
from pyzero_dtq import collector as collector_proc
from pyzero_dtq.application import Application
from unittest.mock import patch, MagicMock
from tests import utils as test_utils, stubs


@pytest.fixture
def dummy_collector():
    with patch.object(collector_proc, 'Sink'):
        with patch.object(collector_proc, 'factory'):
            _collector = collector_proc.Collector(test_utils.TCP_COLLECTOR_URL_SOCKET,
                                                  test_utils.TCP_PUBLISHER_URL_SOCKET,
                                                  10)
            _collector.clean = MagicMock()
    return _collector


@pytest.fixture
def collector():
    with patch.object(collector_proc, 'Sink'):
        _collector = collector_proc.Collector(test_utils.TCP_COLLECTOR_URL_SOCKET,
                                              test_utils.TCP_PUBLISHER_URL_SOCKET,
                                              10)
        _collector.app = stubs.AppStub
    return _collector


def test_finite_loops(dummy_collector):
    '''Ensure that when a collector is given a finite amount of loops, they run at most for as many loops as given
    '''

    dummy_collector.sink.run.return_value = False
    loops = 10
    dummy_collector.run(loops)
    assert dummy_collector.sink.run.call_count == loops


def test_perform_task(dummy_collector):
    '''Ensure that when a collector is given a number of tasks they all are processed as follows;

    1) All tasks are sent to the queue
    2) A worker is only created if there is room for it
    3) The worker_running is increased when a new worker is started
    4) The workers array keep the information about all worker processes created
    '''

    dummy_collector.sink.run.return_value = [mpq_protocol.REQ_DO, {'topic': 'something', 'info': 'something'}]
    loops = 20
    assert dummy_collector.max_workers == 10
    with patch.object(dummy_collector, "start_worker") as mock_start_worker:
        mock_start_worker.return_value = 1
        dummy_collector.run(loops)

    assert dummy_collector.task_queue.send.call_count == loops
    assert mock_start_worker.call_count == 10
    assert dummy_collector.workers_running == 10
    assert sum(dummy_collector.workers) == 10


def test_app_setter_property(dummy_collector):
    '''Ensure than when an app is provided via @app property, this is a subclass of Application
    '''

    with pytest.raises(ValueError):
        dummy_collector.app = MagicMock()

    class AppSubclass(Application):

        def accept(self, criteria):
            pass

        def run(self, task):
            pass
    dummy_collector.app = AppSubclass


def test_clean_for_workers_only(collector):
    '''Ensure that when workers are sent a poison pill they die.
    '''

    # collector.sink.clean() and collector.publisher poison pill are deactivated

    collector.kill_publisher = MagicMock()

    # Start workers with an infinite loop
    for _ in range(10):
        collector.workers.append(multiprocessing.Process(target=collector.start_worker))
        collector.workers[-1].start()
    time.sleep(0.1)
    collector.clean()
    # This line will never run if workers did not receive a poison pill each as they would all be block on the queue
    assert True


def test_clean_for_publisher_only(collector):
    '''Ensure that when a publisher is sent a poison pill they die
    '''

    collector.kill_workers = MagicMock()
    collector.publisher = multiprocessing.Process(target=collector.start_publisher)
    collector.publisher.start()
    time.sleep(0.1)
    collector.clean()
    collector.publisher.join()
    assert True  # This line would not run if the publisher did not receive a poison pill


def test_clean_rest_of_cases(collector):
    '''Ensure clean method behaves as expected as follows:

    1.1) If .sink is an object then call clean()
    1.2) if .workers is empty => kill_workers isn't called
    1.3) if .publisher is empty => kill_workers isn't called
    '''

    collector.kill_workers = MagicMock()
    collector.kill_publisher = MagicMock()

    collector.clean()
    collector.sink.clean.assert_called_once()
    collector.kill_workers.assert_not_called()
    collector.kill_publisher.assert_not_called()
