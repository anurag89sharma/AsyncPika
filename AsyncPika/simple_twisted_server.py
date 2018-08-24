import pika
import json
import time
import random
from common.watcher import timeout
from twisted.internet.task import deferLater
from pika.adapters import twisted_connection
from twisted.internet import defer, reactor, protocol, task
from common.utilities import get_basic_logger, fib
from common.watcher import async_sleep


resp_queue_name = 'response_queue'
request_queue_name = 'request_queue'
logger = get_basic_logger()


@defer.inlineCallbacks
def run(connection, task_number=0):

    channel = yield connection.channel()

    yield channel.basic_qos(prefetch_count=1)

    queue_object, consumer_tag = yield channel.basic_consume(queue=request_queue_name, no_ack=False)

    l = task.LoopingCall(read, queue_object, task_number)

    l.start(0.01)

    logger.info("Task - %s, initialized" % task_number)


@defer.inlineCallbacks
def read(queue_object, task_number=0):

    ch,method,properties,body = yield queue_object.get()
    logger.info('Task: %s, Received message : %s'% (task_number,  body))
    if body:
        message = json.loads(body.decode('utf-8'))
        resp_val = yield process_message(message, task_number)

        message['response'] = json.dumps(resp_val)

        yield ch.basic_publish(exchange='', routing_key=resp_queue_name,
                                    body=json.dumps(message))

    yield ch.basic_ack(delivery_tag=method.delivery_tag)


@defer.inlineCallbacks
def process_message(message, task_number=0):

    try:
        num = int(message.get('payload', {}).get('payload'))
    except Exception as e:
        logger.exception("Error extracting payload from- {}".format(message))
    else:
        logger.info("Task: %s, [.] fib(%s)" % (task_number, num))
        t0 = time.time()
        response = timeout(fib, num, default=-1)
        # response = fib(num)
        yield async_sleep(random.random())
        t1 = time.time()
        logger.info("Task: %s, [.] fib(%s), response - %s, returned in %.4f sec" % (task_number, num, response, t1 - t0))
        response = [{
            "Response": response, "Request": "Fib({})".format(num)
        }]

        defer.returnValue(response)
        yield deferLater(reactor, 0, lambda: None)


def add_async_tasks(cc, task_number=0):
    d = cc.connectTCP('localhost', 5672)
    d.addCallback(lambda protocol: protocol.ready)
    d.addCallback(run, task_number)
    d.addErrback(reconnect)


def reconnect(err):
    print("Error Occurred!!!")

if __name__ == '__main__':
    try:
        parameters = pika.ConnectionParameters()
        cc = protocol.ClientCreator(reactor, twisted_connection.TwistedProtocolConnection,
                                    parameters)
        _ = [add_async_tasks(cc, i) for i in range(5)]

        reactor.run()
        logger.info("Work done Quitting")
    except Exception as e:
        logger.exception(e)