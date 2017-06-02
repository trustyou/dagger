import logging
import multiprocessing

import time


def _run_in_process(task):
    """
    :param task: Task to be run
    :return: True if run successfully, False otherwise.
    """
    # Bound methods are not picklable ... so we need this wrapper as a target function in the process that runs a task
    try:
        task.run()
    except KeyboardInterrupt:
        # Due to http://bugs.python.org/issue8296, letting this exception "bubble" fails to terminate the process pool
        return False
    except Exception as e:
        logging.exception(str(e))
        return False
    else:
        return True


class DaggerException(Exception):
    """
    Execution of a graph of tasks failed. Contains information on successfully completed, pending and failed tasks for
    inspection and possible restart.
    """

    def __init__(self, pending_tasks, done_tasks, failed_tasks):
        """
        :param pending_tasks: List or set of tasks which are pending, i.e. couldn't be done because dependencies failed
        :param done_tasks: Tasks which completed successfully
        :param failed_tasks: Tasks which failed
        """
        self.pending_tasks = pending_tasks
        self.done_tasks = done_tasks
        self.failed_tasks = failed_tasks

    def __str__(self):
        return (
            "{name}\n"
            "- pending tasks: {num_pending}\n"
            "- done tasks: {num_done}\n"
            "- failed tasks: {failed}"
        ).format(
                name=type(self).__name__,
                num_pending=len(self.pending_tasks),
                num_done=len(self.done_tasks),
                failed=", ".join(str(task) for task in self.failed_tasks)
        )


def run_tasks(initial_tasks, pool_size=None, tick=1):
    """
    Run tasks, guaranteeing that their dependencies will be run before them. Work is distributed in a process pool to
    profit from parallelization.

    If one of the tasks fails, all currently running tasks will be run to completion. Afterwards, a DaggerException is
    raised, containing sets of completed, pending and failed tasks.

    :param initial_tasks: Iterable of Task instances.
    :param pool_size: Size of process pool. Default is the number of CPUs
    :param tick: Frequency of dagger ticks in seconds
    """

    pending_tasks = set(initial_tasks)
    for task in initial_tasks:
        task.thread_safe_check_circular_dependencies([])
        pending_tasks |= set(task.get_all_dependencies())
    done_tasks = set()

    return run_partial_tasks(pending_tasks, done_tasks, pool_size, tick)


def run_partial_tasks(pending_tasks, done_tasks, pool_size=None, tick=1):
    """
    Run a graph of tasks where some are already finished. Useful for attempting a rerun of a failed dagger execution.
    """

    num_tasks = len(pending_tasks) + len(done_tasks)

    # On failure of a task, information on the error, and the sate of the DAG is collected in this dictionary
    error_state = {
        "success": True,
        "pending_tasks": set(),
        "done_tasks": set(),
        "failed_tasks": set(),
    }

    pool = multiprocessing.Pool(processes=pool_size)

    def run_task(task):
        logging.info("Running: %s", task)
        pending_tasks.remove(task)

        def task_done(res):
            """
            :param res: True if the task execution was successful
            """
            if res:
                logging.info("Done: %s", task)
                done_tasks.add(task)
            else:
                logging.critical("Failed: %s", task)
                logging.critical("Waiting for completion of: %d tasks",
                                 num_tasks - len(pending_tasks) - len(done_tasks) - 1)

                error_state["success"] = False
                error_state["failed_tasks"].add(task)

                if not error_state["pending_tasks"]:
                    error_state["pending_tasks"] |= pending_tasks
                pending_tasks.clear()

        pool.apply_async(_run_in_process, [task], callback=task_done)
        logging.info("Tasks Status: #pending %d\t#running %d\t#done %d\n",
                     len(pending_tasks),
                     num_tasks - len(pending_tasks) - len(done_tasks),
                     len(done_tasks)
                     )

    while pending_tasks:
        running_tasks = set(
                task for task in pending_tasks
                if all(dep in done_tasks for dep in task.dependencies)
        )
        for task in running_tasks:
            run_task(task)
        time.sleep(tick)

    pool.close()
    pool.join()

    if error_state["success"]:
        logging.info("All tasks are done!")
        return True

    logging.critical("Tasks execution failed")
    error_state["done_tasks"] |= done_tasks
    raise DaggerException(error_state["pending_tasks"], error_state["done_tasks"], error_state["failed_tasks"])
