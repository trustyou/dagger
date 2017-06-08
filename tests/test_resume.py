import pytest

from dagger import run_tasks, Task, DaggerException
from multiprocessing import Array


# arrays in shared memory to test number of task executions
array_faulty = Array("i", [0])
array_extract = Array("i", [0])


class FaultyTask(Task):
    # a task that is implemented with error
    def run(self):
        array_faulty[0] += 1
        None.fail()

# a correct implementation of  a Faulty tasks' run
def new_run(self):
    array_faulty[0] += 20

class ExtractTask(Task):
    def run(self):
        # a task doing some important and long stuff
        array_extract[0] += 1

def test_resume():
    """
    Test the option to persist DAG state across runs in case of failure of
    a task
    """
    extract_1 = ExtractTask({})
    faultyTask = FaultyTask({}, [extract_1])
    extract_2 = ExtractTask({}, [faultyTask])

    # this should fail
    with pytest.raises(DaggerException):
        run_tasks([extract_2], resume_id="test")

    # now we change implementation of FaultyTask run method

    FaultyTask.run = new_run

    # and rerun from where we left of
    extract_1 = ExtractTask({})
    faultyTask = FaultyTask({}, [extract_1])
    extract_2 = ExtractTask({}, [faultyTask])

    run_tasks([extract_2], resume_id="test")
    # assert that we are running a faulty task once, and a correct task once
    assert array_faulty[0] == 21
    # assert that we dont  repeat tasks that are done
    assert array_extract[0] == 2
