import pytest
import time
import multiprocessing

from pymulproc import mpq_protocol, factory
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

    worker.task_queue.receive.return_value = [mpq_protocol.REQ_DO, {'topic': 'something', 'info': 'something'}]
    loops = 10
    worker.run(loops)
    assert worker.task_queue.receive.call_count == loops


def test_poison_pill(worker):
    '''Ensure that when workers are given an instruction to die they do so
    '''
    worker.task_queue.receive.return_value = [mpq_protocol.REQ_DIE]
    loops = 10
    worker.run(loops)
    worker.task_queue.receive.assert_called_once()
    worker.app.run.assert_not_called()


def test_perform_task(worker):
    '''Ensure that when a task is given, the worker used the injected app to perform the task and add teh result
    to the result_queue
    '''

    worker.task_queue.receive.return_value = [mpq_protocol.REQ_DO]
    loops = 1
    worker.run(loops)
    worker.task_queue.receive.assert_called_once()
    worker.app.run.assert_called_once()
    worker.result_queue.send.assert_called_once()


def test_block_when_queue_is_empty(worker):
    '''Ensure that workers block when the queue is empty but is awaiting for the next message to be popped in
    '''

    queue_factory = factory.QueueCommunication()
    worker.task_queue = queue_factory.parent()  # Now the worker will block in the queue

    def add_message_to_queue_after_delay(q_factory):
        '''Sends a message to the queue for the picker to die right away
        '''
        time.sleep(1)
        child = q_factory.child()
        child.send(mpq_protocol.REQ_DIE)

    child_process = multiprocessing.Process(target=add_message_to_queue_after_delay, args=(queue_factory,))
    child_process.start()
    message = worker.run()
    # If the worker would have not picked the message sent by the child, it would block indefinitely in the line below
    worker.task_queue.queue_join()
    assert message is not False
