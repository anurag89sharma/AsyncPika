from multiprocessing.managers import SyncManager


class Fibonacci:
    def __init__(self):
        self.__cache = dict()

    def get(self, n):
        if n in self.__cache:
            return self.__cache[n]
        else:
            self.__cache[n] = n if n < 2 else self.get(n-2) + self.get(n-1)
            return self.__cache[n]

    def get_cache(self):
        return self.__cache

    def lookup_cache(self, n):
        return self.__cache.get(n)

class MyManager(SyncManager):
    """
    This Manager is responsible for coordinating shared
    information state between all processes
    """
    pass

# Register your custom "Counter" class with the manager
MyManager.register('Fibonacci', Fibonacci)


def Manager():
    m = MyManager()
    m.start()
    return m

if __name__ == '__main__':
    from common.watcher import get_basic_logger
    logger = get_basic_logger()
    obj = Fibonacci()
    resp = obj.get(5)
    print(resp)