import sys
import json
import random
import asyncio
import aioamqp
from common.utilities import get_basic_logger

logger = get_basic_logger()
resp_queue_name = 'response_queue'
request_queue_name = 'request_queue'
num_messages_to_send = 5
count = 0

@asyncio.coroutine
def on_request(channel, body, envelope, properties):
    resp = json.loads(body.decode('utf-8'))
    resp_val = resp.get("response")
    logger.info(" [x] Received {}".format(resp_val))
    yield from channel.basic_client_ack(delivery_tag=envelope.delivery_tag)
    global count
    count += 1
    if count == num_messages_to_send:
        sys.exit(0)

@asyncio.coroutine
def call():
    transport, protocol = yield from aioamqp.connect()
    channel = yield from protocol.channel()
    yield from channel.basic_consume(on_request, queue_name=resp_queue_name)
    yield from channel.basic_qos(prefetch_count=1, prefetch_size=0, connection_global=False)

    for i in range(num_messages_to_send):
        payload = random.randint(0, 35)
        message = {
            'payload': {
                'payload': payload,
                'title': 'job_title',
                'description': 'Some job description'
            }
        }

        yield from channel.basic_publish(
            payload=json.dumps(message),
            exchange_name='',
            routing_key=request_queue_name
        )

        logger.info(" [x] Sent {}".format(message))



if __name__ == '__main__':
    event_loop = asyncio.get_event_loop()
    event_loop.run_until_complete(call())
    event_loop.run_forever()
