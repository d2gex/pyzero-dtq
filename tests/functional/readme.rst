=============================
Functional Test Description
=============================

The test taking place here is testing the whole system from a functional point of view. There are a total 33 processes
being involved as follows:

1.  10 producers
2.  10 subscribers
3.  10 workers
4.  1 publisher
5.  1 collector
6.  Main Test process

The idea is to simulate what the system in production would look like with different clients connecting and sending
tasks to the distributed task queue, and this returning back the results of executing such tasks. The simulation goes
like this:

1.  **Collector**: A collector process is started which in turn will start too a ``Publisher`` child process. It will
    run a total of 500 loops, where each loops pools for 10 milliseconds, hence a total operation time of 5 seconds.
    ``Workers`` will be started as tasks are being poured in. When the collector runs out of loops, it will then issue
    a ``DIE`` request to both workers and publisher for them to die right away. The collector lastly will wait until all
    child processes have passed away.
2.  **Publisher**: The publisher process is initiated by the parent collector process and it will run indefinitely
    until its parent issues a ``DIE`` request. At such point it will release the underlying socket.
3.  **Worker**: Similarly to the publisher, workers are initiated by the collector parent process and will run
    indefinitely until its parent process issues a ``DIE`` request.
4.  **subscribers**: 10 subscribers are started and ran for the same amount of loops as the collector, each
    waiting for the messages sent by the publisher process. After 5 seconds, they will release their underlying
    socket too.
5.  **Producers**: 10 producers will send 10 tasks to the collector. Once they have sent the task through their
    socket they will wait alive for sometime to give underlying middleware time to ensure the message is deliver
    to the other side. This is achieved by adding a time.sleep(0.1)
6.  **Main Test Process**: The main test process is sharing a queue with the ``subscribers`` so the latter each sends
    to such queue the results in turn sent by the publisher. With all these data, the main parent process will assert
    that the results received are the ones expected.

Below there is an activity diagram tha shows the interaction among the processes involved in the functional
test:

.. image:: docs/images/pyzero-mq-functional-testing.png
    :alt: Activity diagram that shows how functional tests is executed
    :target: #