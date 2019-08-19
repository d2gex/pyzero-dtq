import pytest
import time
import multiprocessing

from pymulproc import mpq_protocol, factory
from pyzero_dtq import publisher as pub_proc
from unittest.mock import patch, MagicMock


@pytest.fixture
def publisher():
    ''' Create a publisher process whose attributes .result_queue and .publisher are mocked
    '''
    with patch.object(pub_proc.zmq_pub, 'Publisher'):
        pub = pub_proc.Publisher(url='tcp://localhost', identity='Producer_1')
    pub.result_queue = MagicMock()
    return pub


def test_finite_loops(publisher):
    '''Ensure that when a publisher is given a finite amount of loops, they run at most for as many loops as given
    '''

    publisher.result_queue.receive.return_value = [mpq_protocol.REQ_DO, {'topic': 'something', 'info': 'something'}]
    loops = 10
    publisher.run(loops)
    assert publisher.result_queue.receive.call_count == loops


def test_poison_pill(publisher):
    '''Ensure that when a publisher is given an instruction to die they do so
    '''
    publisher.result_queue.receive.return_value = [mpq_protocol.REQ_DIE]
    loops = 10
    publisher.run(loops)
    publisher.result_queue.receive.assert_called_once()


def test_perform_task(publisher):
    '''Ensure that when a task is given by a worker, the publisher sends it over the wire by the publisher end
    '''

    data = {'topic': 'something', 'info': 'something'}
    publisher.result_queue.receive.return_value = [mpq_protocol.REQ_DO, data]
    loops = 1
    publisher.run(loops)
    assert publisher.result_queue.receive.call_count == loops
    publisher.pub.run.assert_called_once_with(data['topic'], data['info'])


def test_block_when_queue_is_empty(publisher):
    '''Ensure that the publisher blocks when the queue is empty but is awaiting for the next message to be popped in
    '''

    queue_factory = factory.QueueCommunication()
    publisher.result_queue = queue_factory.parent()  # Now the publisher will block in the queue

    def add_message_to_queue_after_delay(q_factory):
        '''Sends a message to the queue for the picker to die right away
        '''
        time.sleep(1)
        child = q_factory.child()
        child.send(mpq_protocol.REQ_DIE)

    child_process = multiprocessing.Process(target=add_message_to_queue_after_delay, args=(queue_factory,))
    child_process.start()
    message = publisher.run()
    # If the publisher would have not picked the message sent by the child, it would block indefinitely
    # in the line below
    publisher.result_queue.queue_join()
    assert message is not False
