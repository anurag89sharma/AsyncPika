# AsyncPika

Asynchronous implementation of RabbitMq publisher in python.

Implemented using twisted and pika library. 

Creates several auto-reconnecting (async)clients with an exponential back-off.

Uses Python multiprocessing library to launch multiple such clients to leverage system cores.
