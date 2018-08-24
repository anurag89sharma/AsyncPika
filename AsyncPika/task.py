import sys
import json
import time
import random
import logging
from pika import exceptions
from twisted.python import log
from twisted.internet import defer
from twisted.internet import reactor, task
from common.watcher import timeout, async_sleep


log.startLogging(sys.stdout)
resp_queue_name = 'response_queue'
request_queue_name = 'request_queue'
task_state = ('RUNNING', 'CLEAN-SHUTDOWN')


class Task:
    def __init__(self, task_number=0, calling_func=None, logger=None):
        self.task_number = task_number
        self.calling_func = calling_func
        self.logger = logger if logger else self.get_basic_logger()
        self.twistd_log = log.startLogging(sys.stdout)
        self.looping_call = None
        self.task_state = task_state[0]
        self.ok_to_terminate= True

    def get_basic_logger(self):
        logging.basicConfig(
            format='[%(asctime)s - %(filename)s - %(processName)s] - %(lineno)d - %(levelname)s - %(message)s',
            level=logging.INFO
        )
        logger = logging.getLogger(__name__)
        return logger

    def is_task_terminated(self):
        return self.ok_to_terminate

    def terminate_task(self):
        self.logger.info("Task - %s, Termination signal received!!!" % self.task_number)
        self.task_state = task_state[1]
        self.logger.info("Task: %s, Terminating Loop!!! Good Bye" % self.task_number)
        self.looping_call.stop()

    @defer.inlineCallbacks
    def run(self, connection):
        channel = yield connection.channel()

        yield channel.basic_qos(prefetch_count=1)

        queue_object, consumer_tag = yield channel.basic_consume(queue=request_queue_name, no_ack=False)

        from twisted.internet import task
        self.looping_call = task.LoopingCall(self.handle, queue_object)

        self.looping_call.start(0.01).addErrback(log.err)

        self.logger.info("Task - %s, initialized" % self.task_number)

    @defer.inlineCallbacks
    def handle(self, queue_object):
        try:
            ch, method, properties, body = yield queue_object.get()
        except exceptions.ChannelClosed as e:
            # The channel was closed unexpectedly. PikaFactory will
            # automatically try to reconnect to the rabbit-mq client
            pass
        else:
            self.ok_to_terminate = False
            self.logger.info('Task: %s, Received message : %s' % (self.task_number, body))
            if body:
                message = json.loads(body.decode('utf-8'))
                resp_val = yield self.process_message(message)

                message['response'] = json.dumps(resp_val)

                yield ch.basic_publish(exchange='', routing_key=resp_queue_name,
                                       body=json.dumps(message))
            yield ch.basic_ack(delivery_tag=method.delivery_tag)
        finally:
            self.ok_to_terminate = True

    @defer.inlineCallbacks
    def process_message(self, message):
        from common.utilities import fib
        response = []
        try:
            num = int(message.get('payload', {}).get('payload'))
        except Exception as e:
            self.logger.exception("Error extracting payload from- {}".format(message))
        else:
            self.logger.info("Task: %s, [.] fib(%s)" % (self.task_number, num))
            t0 = time.time()
            if self.calling_func:
                # logger.info("Shared Cache: %s" % func.get_cache())
                response = timeout(self.calling_func.get, num, default=-1, logger=self.logger)
            else:
                response = timeout(fib, num, default=-1, logger=self.logger)
            # response = fib(num)
            yield async_sleep(random.random())
            t1 = time.time()
            self.logger.info(
                "Task: %s, [.] fib(%s), response - %s, returned in %.4f sec" % (self.task_number, num, response, t1 - t0))
            response = [{
                "Response": response, "Request": "Fib({})".format(num)
            }]

        defer.returnValue(response)

        yield task.deferLater(reactor, 0, lambda: None)
