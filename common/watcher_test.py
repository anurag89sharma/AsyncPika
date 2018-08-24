import random
import multiprocessing
from common.watcher import timeout, some_time_taking_function,\
    some_normal_function, get_basic_logger

logger = get_basic_logger()


def workers():
    timout = random.randint(3, 9)
    logger.info("Initiated worker with timeout - {}".format(timout))
    result = timeout(some_time_taking_function, timeout_duration=timout)
    logger.info("Response: {}".format(result))

def handle_error(e):
    print(e)

def main():
    results = []
    num_process = 2
    pool = multiprocessing.Pool(num_process)
    for i in range(num_process):
        job = pool.apply_async(workers, error_callback=handle_error)
        results.append(job)

    pool.close()
    pool.join()


if __name__ == "__main__":
    main()
