import time

import pytest

from dagger import DaggerException, run_tasks, Task


class CrawlTask(Task):

    def run(self):
        if self.config["fail"]:
            time.sleep(2)
            None.fail()
        else:
            time.sleep(1)


class ExtractTask(Task):

    def run(self):
        time.sleep(1)


def test_failure():

    blessed_crawl = CrawlTask({"fail": False})
    doomed_crawl = CrawlTask({"fail": True})

    extract_1 = ExtractTask({}, [blessed_crawl])
    extract_2 = ExtractTask({}, [doomed_crawl])

    with pytest.raises(DaggerException) as exc_info:
        run_tasks([extract_1, extract_2])

    ex = exc_info.value
    assert set([doomed_crawl]) == ex.failed_tasks
    assert set([blessed_crawl, extract_1]) == ex.done_tasks
    assert set([extract_2]) == ex.pending_tasks