import time
import signal
import asyncio
from twisted.internet import defer, reactor

def async_sleep(secs):
  d = defer.Deferred()
  reactor.callLater(secs, d.callback, None)
  return d

# comment below if using rabbitmq via pika or any other blocking library
# @asyncio.coroutine
def timeout(func, *args, timeout_duration=5, default=None, logger=None, **kwargs):

    class TimeoutError(Exception):
        pass

    def handler(signum, frame):
        raise TimeoutError()

    # set the timeout handler
    signal.signal(signal.SIGALRM, handler)
    signal.alarm(timeout_duration)

    from common.utilities import get_basic_logger
    if not logger: logger = get_basic_logger()

    try:
        result = func(*args, **kwargs)
    except TimeoutError as exc:
        logger.error("Timeout of {}secs reached while executing function - {}".format(timeout_duration, func.__name__))
        result = default
    except Exception:
        logger.exception("Generic exception reached while saving screenshot")
        result = default
    finally:
        signal.alarm(0)

    return result

def some_time_taking_function(*args, **kwargs):
    time.sleep(10)
    return "Hello"

def some_normal_function(*args, **kwargs):
    time.sleep(1)
    return "Hello World"


if __name__ == "__main__":
    from common.utilities import get_basic_logger
    logger = get_basic_logger()
    args = ('python','eggs','test')
    kwargs = {"arg1": 1, "arg2": "two","arg3":3}
    # result = timeout(some_time_taking_function, timeout_duration=5, default=0, *args, **kwargs)
    result = timeout(some_time_taking_function, *args, **kwargs)
