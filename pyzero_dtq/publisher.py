from pymulproc import mpq_protocol
from pyzero_dtq.iprocess import IProcess
from pubsub_zmq import publisher as zmq_pub


class Publisher(IProcess):

    def __init__(self, url, identity):
        super().__init__()
        self.url = url
        self.identity = identity
        self._result_queue = None
        self.pub = zmq_pub.Publisher(url=url, identity=identity)

    @property
    def result_queue(self):
        return self._result_queue

    @result_queue.setter
    def result_queue(self, result_q):
        self._result_queue = result_q

    def run(self, loops=True):
        '''Block in shared queue until messages popped in. Then it will send them over via its publisher socket unless
        an indication to die has been received.
        '''
        stop = False

        try:
            while not stop and loops:
                result = self.result_queue.receive(block=True)
                if result[mpq_protocol.S_PID_OFFSET - 1] == mpq_protocol.REQ_DIE:
                    stop = True
                else:
                    data = result[-1]
                    topic = data['topic']
                    info = data['info']
                    self.pub.run(topic, info)

                if not stop and not isinstance(loops, bool):
                    loops -= 1
        except KeyboardInterrupt:
            pass
        finally:
            self.clean()

    def clean(self):
        if self.pub:
            self.pub.clean()
