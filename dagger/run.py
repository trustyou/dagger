import logging
import multiprocessing

import time

import pickle

from os.path import isfile
from os import remove

def save_state(state, filename):
    """
    :param state: dictionary containg current dag state
    :param filename: filename to save into
    """
    logging.info("Saving DAG state into {}...".format(filename))
    with open(filename, 'wb') as writefile:
        pickle.dump(state, writefile)
    logging.info("Done! Run 'run tasks' with the same id flag to pick up")

def load_state(filename):
    """
    :param filename: filename to read from
    :return: dictionary containing DAG state
    """
    logging.info("Loading DAG state from {}...".format(filename))

    with open(filename, 'rb') as readfile:
        recovered_state = pickle.load(readfile)
    return recovered_state

def get_filename(id_string):
    """
    :param id_string: id to turn into filename
    :return: properly formated filename
    """
    id_string = id_string.replace(" ", "_")
    return "{id_string}.dump"


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


def run_tasks(initial_tasks, pool_size=None, tick=1, resume_id = ''):
    """
    Run tasks, guaranteeing that their dependencies will be run before them. Work is distributed in a process pool to
    profit from parallelization.

    If one of the tasks fails, all currently running tasks will be run to completion. Afterwards, a DaggerException is
    raised, containing sets of completed, pending and failed tasks.

    If the resume id is set the next time run_tasks with the same id is called, Dagger will try to pick up the
    previous state and skip running all the tasks that were completed last time.

    :param initial_tasks: Iterable of Task instances.
    :param pool_size: Size of process pool. Default is the number of CPUs
    :param tick: Frequency of dagger ticks in seconds
    :param resume_id: Id of the DAG to trigger resuming from an old state
    """

    if resume_id and isfile(get_filename(resume_id)):
        # if we have an id set and a dump file, we try to resume from previous state
        logging.info("recovering from a previously saved state...")
        recovered_state = load_state(get_filename(resume_id))
        initial_tasks = recovered_state['pending_tasks'] | recovered_state['failed_tasks']
        done_tasks = recovered_state['done_tasks']
        pending_tasks = set(initial_tasks)
    else:
        # if not, we start from scratch
        pending_tasks = set(initial_tasks)
        done_tasks = set()
        for task in initial_tasks:
            task.check_circular_dependencies([])
            pending_tasks |= set(task.get_all_dependencies())

    return run_partial_tasks(pending_tasks, done_tasks, pool_size, tick, resume_id)


def run_partial_tasks(pending_tasks, done_tasks, pool_size=None, tick=1, resume_id = ''):
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
                logging.critical("Waiting for completion of: %d tasks", num_tasks - len(pending_tasks) - len(done_tasks) - 1)

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
        if resume_id and isfile(get_filename(resume_id)):
            # if we successfully completed everything, remove the dump if its present
            logging.info("Removing previously created state")
            remove(get_filename(resume_id))
        return True

    logging.critical("Tasks execution failed")
    error_state["done_tasks"] |= done_tasks

    if resume_id:
        # pickle the state to resume from it later if the id is provided
        save_state(error_state, get_filename(resume_id))

    raise DaggerException(error_state["pending_tasks"], error_state["done_tasks"], error_state["failed_tasks"])
