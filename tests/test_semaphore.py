from multiprocessing import BoundedSemaphore, Lock, Value
import time

from dagger import run_tasks, Task

counter = Value("i", 0)
lock = Lock()
max_concurrent = 2
semaphore = BoundedSemaphore(max_concurrent)


class DumpTask(Task):
    """
    Task that simulates a mixture of parallelizable code, and a portion where a limited resource (protected by a
    semaphore) is accessed.
    """

    def run(self):
        time.sleep(1)

        print("Waiting ...", self.config)
        semaphore.acquire()

        # atomic inc
        lock.acquire()
        counter.value += 1
        lock.release()

        time.sleep(1)
        assert counter.value <= max_concurrent

        # atomic dec
        lock.acquire()
        counter.value -= 1
        lock.release()

        semaphore.release()
        print("Done!", self.config)

        time.sleep(1)


def almost_equal(x, y, prec=0.1):
    return abs(x -y) <= prec


def test_semaphore():
    """
    Test using a semaphore to limit the number of tasks concurrently accessing a resource. Running this graph is
    expected to take 7 seconds.
    """
    start = time.time()
    tasks = list(DumpTask(i) for i in range(10))
    run_tasks(tasks, pool_size=10)
    end = time.time()

    assert almost_equal(end - start, 7)
