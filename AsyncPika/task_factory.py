import os
import sys
import pika
import logging
from twisted.internet import protocol, reactor, task
from pika.adapters import twisted_connection
from AsyncPika.task import Task

CONNECTION_PARAMETERS = {}
total_tasks = 5


def get_basic_logger():
    logging.basicConfig(
        format='[%(asctime)s - %(filename)s - %(processName)s] - %(lineno)d - %(levelname)s - %(message)s',
        level=logging.INFO
    )
    logger = logging.getLogger(__name__)
    return logger


class PikaFactory(protocol.ReconnectingClientFactory):
    def __init__(self, parameters, task_number=0, calling_func=None, logger=None):
        self.parameters = parameters
        self.task_count = total_tasks
        self.task_number = task_number
        self.factor = 1.5
        self.calling_func = calling_func
        self.logger = logger if logger else get_basic_logger()
        self.task_objects = []

    def startedConnecting(self, connector):
        self.logger.info('Task: %s, Started to connect.' % self.task_number)

    def buildProtocol(self, addr):
        self.resetDelay()
        self.logger.info('Task: %s, Connected' % self.task_number)
        proto = twisted_connection.TwistedProtocolConnection(self.parameters)
        task_obj = Task(self.task_number, self.calling_func, self.logger)
        self.task_objects.append(task_obj)
        proto.ready.addCallback(task_obj.run)
        self.task_number += 1
        return proto

    def clientConnectionLost(self, connector, reason):
        self.task_number -= 1
        self.logger.error('Task: %s, Lost connection.  Reason: %s' % (self.task_number, reason))
        protocol.ReconnectingClientFactory.clientConnectionLost(
                 self, connector, reason)

    def clientConnectionFailed(self, connector, reason):
        self.logger.error('Task: %s, Connection failed. Reason: %s' % (self.task_number, reason))
        protocol.ReconnectingClientFactory.clientConnectionFailed(
                 self, connector, reason)

    def all_tasks_terminated(self):
        return all([obj.is_task_terminated() for obj in self.task_objects])

    def stop_tasks(self, val, lock):
        self.logger.info("Checking Tasks termination condition!!!")
        if val.value >= 1:
            self.logger.info("Initiating Tasks termination!!!")
            for obj in self.task_objects:
                obj.terminate_task()

            while True:
                if self.all_tasks_terminated():
                    self.continueTrying = 0
                    self.logger.info("All tasks terminated. Stopping the reactor!!!")
                    if reactor.running: reactor.stop()
                    break

            with lock:
                self.logger.info("Incrementing val.value by 1, current value - %s" % val.value)
                val.value += 1


def run_tasks(calling_func, val, lock):
    from common.utilities import get_logger
    logger = get_logger(fname='/backup/rabbit.log.out')
    try:
        parameters = pika.ConnectionParameters(**CONNECTION_PARAMETERS)
        factory = PikaFactory(parameters, 0, calling_func, logger)

        for i in range(total_tasks):
            reactor.connectTCP(parameters.host, parameters.port, factory)

        logger.info(' [*] Waiting for messages. To exit press CTRL+C')
        looping_call = task.LoopingCall(factory.stop_tasks, val, lock)
        looping_call.start(5)
        reactor.run()
    except:
        logger.exception("Error")
        if reactor.running: reactor.stop()

    logger.info("Process Finished - %s" % os.getpid())

if __name__ == '__main__':
    class Val:
        __slots__ = ['value']

        def __init__(self, n):
            self.value = n

    val = Val(0)
    # obj = Fibonacci()
    # run_tasks(obj)
    run_tasks(None, val)
