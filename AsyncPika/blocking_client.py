import sys
import pika
import json
from random import randint
from common.utilities import get_basic_logger

logger = get_basic_logger()
resp_queue_name = 'response_queue'
request_queue_name = 'request_queue'

connection = pika.BlockingConnection(pika.ConnectionParameters(
               'localhost'))
channel = connection.channel()
# arguments={'x-message-ttl': 60000}
channel.queue_declare(queue=request_queue_name, durable=True)
channel.queue_declare(queue=resp_queue_name, durable=True)


def callback(ch, method, props, body):
    resp = json.loads(body.decode('utf-8'))
    resp_val = resp.get("response")
    logger.info(" [x] Received {}".format(resp_val))
    ch.basic_ack(delivery_tag=method.delivery_tag)
    sys.exit(0)

try:
    payload = int(sys.argv[1])
except Exception as e:
    payload = str(randint(0, 35))

message = {
    'payload': {
        'payload': payload,
        'res_id': '12345',
        'title': 'job_title',
        'description': 'Some job description'
    }
}
channel.basic_publish(exchange='',
                      routing_key=request_queue_name,
                      body=json.dumps(message))
logger.info(" [x] Sent {}".format(message))

channel.basic_consume(callback, queue=resp_queue_name)
logger.info(" [x] Waiting for messages. To exit press CTRL+C")

try:
    channel.start_consuming()
except KeyboardInterrupt:
    logger.error(' [*] Exiting...')

connection.close()