import pytest

from dagger import Task, DaggerException, run_tasks
from dagger.task import CircularDependencyException


class TaskTest(Task):
    """
    Inherited Task class,
    run method is NotImplemented.
    """

    def run(self):
        """
        :return: None
        """
        print("Hello world! I am %s" % self.config["name"])


def test_acyclic_dependency():
    """
    You have tasks A,B,C,D
    with the following dependencies.

    A ---> B ---> C
    A ---> D ---> C

    It should run tasks with no error
    """
    C = TaskTest({"name": "C"}, [])

    B = TaskTest({"name": "B"}, [C])

    D = TaskTest({"name": "D"}, [C])

    A = TaskTest({"name": "A"}, [B, D])

    assert run_tasks([A, D]) is True


def test_cyclic_dependency():
    """
    You have tasks A,B,C
    with the following dependencies.

    A ---> B ---> C ---> A

    B ---> C

    C

    It should raise CircularDependencyException
    due to circular dependency on A task.
    """
    C = TaskTest({"name": "C"}, [])

    B = TaskTest({"name": "B"}, [C])

    A = TaskTest({"name": "A"}, [B])

    C.dependencies.append(A)

    A.dependencies.append(C)

    with pytest.raises(CircularDependencyException) as exc_info:
        run_tasks([A, B, C])

    assert exc_info.value.tasks_chain == [A, B, C, A]
