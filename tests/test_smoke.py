#!/usr/bin/env python

from multiprocessing import Array, BoundedSemaphore, Lock, Value
from time import sleep, time

from dagger import run_tasks, Task


# array in shared memory to test cooperation of tasks
array = Array("i", 1)


class SetTask(Task):
    """
    Set a value in the shared array.
    """

    def run(self):
        array[self.config["index"]] = self.config["value"]


class IncTask(Task):
    """
    Increment a value in the shared array.
    """
    def run(self):
        array[self.config["index"]] += 1


def test_smoke():

    set_task = SetTask({"index": 0, "value": 41})
    inc_task = IncTask({"index": 0}, [set_task])

    res = run_tasks([inc_task], pool_size=1)

    assert res is True
    assert array[0] == 42

    inc_again_task = IncTask({"index": 0})
    res = run_tasks([inc_again_task], pool_size=10)

    assert res is True
    assert array[0] == 43