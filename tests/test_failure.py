import time
from multiprocessing import Value as SharedMemoryValue

import pytest

from dagger import DaggerException, run_tasks, Task


# Share failure counter between processes to correctly count down the failed attempts.
# Need to create it globally in order to inherit it into child processes.
failure_counter = SharedMemoryValue("i", 0, lock=False)


class CrawlTask(Task):

    def __init__(self, failures=0, retries=None):
        if failures:
            failure_counter.value = failures

        super(CrawlTask, self).__init__(
            config={'fail_and_retry': failures > 0},
            retries=retries)

    def run(self):
        if self.config['fail_and_retry'] and failure_counter.value > 0:
            failure_counter.value -= 1
            time.sleep(2)
            None.fail()
        else:
            time.sleep(1)


class UnsafeRetriedCrawlTask(CrawlTask):
    """
    Used to verify that any changes in the task instance state do not leak
    into the next execution.
    """
    DEFAULT_RETRIES = 2
    SANE_STATE = True

    def run(self):
        if not self.SANE_STATE:
            raise RuntimeError("unsafe state change detected")
        self.SANE_STATE = False
        super(UnsafeRetriedCrawlTask, self).run()


class ExtractTask(Task):

    def run(self):
        time.sleep(1)


def test_failure():

    blessed_crawl = CrawlTask(failures=0)
    doomed_crawl = CrawlTask(failures=1)

    extract_1 = ExtractTask({}, [blessed_crawl])
    extract_2 = ExtractTask({}, [doomed_crawl])

    assert doomed_crawl.retries_on_failure == 0
    with pytest.raises(DaggerException) as exc_info:
        run_tasks([extract_1, extract_2])
    assert doomed_crawl.retries_on_failure == 0

    ex = exc_info.value
    assert set([doomed_crawl]) == ex.failed_tasks
    assert set([blessed_crawl, extract_1]) == ex.done_tasks
    assert set([extract_2]) == ex.pending_tasks


def test_retry_on_failure_with_success():
    """
    Test that retrying leads to success if the last retry succeeds
    """
    blessed_crawl = CrawlTask(failures=0)
    retried_crawl = CrawlTask(failures=3, retries=3)

    extract_1 = ExtractTask({}, [blessed_crawl])
    extract_2 = ExtractTask({}, [retried_crawl])

    assert blessed_crawl.retries_on_failure == 0
    assert retried_crawl.retries_on_failure == 3
    result = run_tasks([extract_1, extract_2])
    assert blessed_crawl.retries_on_failure == 0
    assert retried_crawl.retries_on_failure == 0
    assert result


def test_retry_on_one_failure_with_success():
    """
    Test that retrying leads to success if any retry succeeds
    """
    blessed_crawl = CrawlTask(failures=0)
    retried_crawl = CrawlTask(failures=1, retries=3)

    extract_1 = ExtractTask({}, [blessed_crawl])
    extract_2 = ExtractTask({}, [retried_crawl])

    assert blessed_crawl.retries_on_failure == 0
    assert retried_crawl.retries_on_failure == 3
    result = run_tasks([extract_1, extract_2])
    assert blessed_crawl.retries_on_failure == 0
    assert retried_crawl.retries_on_failure == 2
    assert result


def test_retry_on_failure_with_abort():

    blessed_crawl = CrawlTask(failures=0)
    retried_crawl = UnsafeRetriedCrawlTask(failures=3)

    extract_1 = ExtractTask({}, [blessed_crawl])
    extract_2 = ExtractTask({}, [retried_crawl])

    assert retried_crawl.retries_on_failure == UnsafeRetriedCrawlTask.DEFAULT_RETRIES > 0
    with pytest.raises(DaggerException) as exc_info:
        run_tasks([extract_1, extract_2])
    assert retried_crawl.retries_on_failure == 0

    ex = exc_info.value
    assert set([retried_crawl]) == ex.failed_tasks
    assert set([blessed_crawl, extract_1]) == ex.done_tasks
    assert set([extract_2]) == ex.pending_tasks
