import pytest

from pymulproc import mpq_protocol
from pyzero_dtq.worker import Worker
from unittest.mock import MagicMock


@pytest.fixture
def worker():
    _worker = Worker()
    _worker.app = MagicMock()
    _worker.task_queue = MagicMock()
    _worker.result_queue = MagicMock()
    return _worker


def test_finite_loops(worker):
    '''Ensure that when workers are given a finite amount of loops, they run at most for as many loops as given
    '''

    worker.task_queue.receive.return_value = False
    loops = 10
    worker.run(loops)
    assert worker.task_queue.receive.call_count == loops


def test_poison_pill(worker):
    '''Ensure that when workers are given an instruction to die they do so
    '''
    worker.task_queue.receive.return_value = [mpq_protocol.REQ_DIE]
    loops = 10
    worker.run(loops)
    worker.app.fetch.assert_called_once()
    worker.task_queue.receive.assert_called_once()
    worker.app.run.assert_not_called()


def test_perform_task(worker):
    '''Ensure that when a task is given, the worker used the injected app to perform the task and add teh result
    to the result_queue
    '''

    worker.task_queue.receive.return_value = [mpq_protocol.REQ_DO]
    loops = 1
    worker.run(loops)
    worker.app.fetch.assert_called_once()
    worker.task_queue.receive.assert_called_once()
    worker.app.run.assert_called_once()
    worker.result_queue.send.assert_called_once()
