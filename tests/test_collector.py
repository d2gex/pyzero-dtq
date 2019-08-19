import pytest

from pymulproc import mpq_protocol
from pyzero_dtq import collector as collector_proc
from unittest.mock import patch, call


@pytest.fixture
def collector():
    with patch.object(collector_proc, 'Sink'):
        with patch.object(collector_proc, 'factory'):
            _collector = collector_proc.Collector('tcp://localhost:5556', 'tcp://localhost:5557', 10)
    return _collector


def test_finite_loops(collector):
    '''Ensure that when a collector is given a finite amount of loops, they run at most for as many loops as given
    '''

    collector.sink.run.return_value = False
    loops = 10
    collector.run(loops)
    assert collector.sink.run.call_count == loops


def test_perform_task(collector):
    '''Ensure that when a collector is given a number of tasks they all are processed as follows;

    1) All tasks are sent to the queue
    2) A worker is only created if there is room for it
    3) The worker_running is increased when a new worker is started
    4) The workers array keep the information about all worker processes created
    '''

    collector.sink.run.return_value = [mpq_protocol.REQ_DO, {'topic': 'something', 'info': 'something'}]
    loops = 20
    assert collector.max_workers == 10
    with patch.object(collector, "start_worker") as mock_start_worker:
        mock_start_worker.return_value = 1
        collector.run(loops)

    assert collector.task_queue.send.call_count == loops
    assert mock_start_worker.call_count == 10
    assert collector.workers_running == 10
    assert sum(collector.workers) == 10
