from pymulproc import mpq_protocol
from pyzero_dtq.iprocess import IProcess


class Worker(IProcess):

    def __init__(self):
        super().__init__()
        self._task_queue = None
        self._result_queue = None
        self._app = None

    @property
    def task_queue(self):
        return self._task_queue

    @task_queue.setter
    def task_queue(self, tas_q):
        self._task_queue = tas_q

    @property
    def result_queue(self):
        return self._result_queue

    @result_queue.setter
    def result_queue(self, result_q):
        self._result_queue = result_q

    @property
    def app(self):
        return self._app

    @app.setter
    def app(self, application):
        self._app = application

    def run(self, loops=True):
        stop = False

        try:
            while not stop and loops:

                task = self.task_queue.receive(func=self.app.accept, block=True)
                if task[mpq_protocol.S_PID_OFFSET - 1] == mpq_protocol.REQ_DIE:
                    stop = True
                else:
                    result = self.app.run(task[-1])
                    self.result_queue.send(mpq_protocol.REQ_DO, data=result)

                if not stop and not isinstance(loops, bool):
                    loops -= 1
        except KeyboardInterrupt:
            pass