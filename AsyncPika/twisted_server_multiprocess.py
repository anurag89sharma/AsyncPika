import os
import sys
import time
import multiprocessing
from common.SharedFibonacci import Manager
from common.utilities import get_basic_logger, get_logger

workers = 4
total_process = workers + 1
# logger = get_basic_logger()

logger = get_logger(fname='/backup/rabbit.log.out')


def launch_process(calling_func, val, lock):
    logger.info("Inside Launch Process with PId - %s" % os.getpid())
    from AsyncPika.task_factory import run_tasks
    run_tasks(calling_func, val, lock)


def handle_error(e):
    logger.exception(e)


def terminate_handler(val):
    logger.info("PID - %s Checking for stop.txt" % os.getpid())
    while val.value == 0:
        # logger.info("stop instruction -> "+ str(os.path.isfile("stop.txt")))
        if os.path.isfile("stop.txt"):
            val.value = 1
            break
        time.sleep(10)
    logger.info("Terminate handler finished!!!")


manager = Manager()
val = manager.Value('i', 0)
lock = manager.Lock()
fibonacci = manager.Fibonacci()
pool = multiprocessing.Pool(processes=total_process)


def pool_terminate(result):
    while 1:
        if val.value >= total_process:
            logger.info("Workers exited - %s, Terminating the pool !!" % workers)
            pool.terminate()
            break
        else:
            time.sleep(10)
    logger.info("Pool terminated")


def run_multiple_processes():

    try:
        worker_pool = [pool.apply_async(launch_process, (fibonacci, val, lock), error_callback=handle_error)
                       for i in range(0, workers)]

        pool.apply_async(terminate_handler, (val, ), error_callback=handle_error, callback=pool_terminate)
        logger.info("PId - %s, Total workers - %s" % (os.getpid(), len(worker_pool)))
        result = [g.get() for g in worker_pool]
        logger.info("All workers finished !!!")

    except KeyboardInterrupt:
        logger.error(' [*] Exiting...')
    finally:
        logger.error(' [*] Exiting...')
        pool.close()
        pool.join()


if __name__ == '__main__':

    run_multiple_processes()