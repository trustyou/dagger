class CircularDependencyException(Exception):
    """
    Circular dependencies error between tasks.
    """

    def __init__(self, tasks_chain):
        """
        :param repeated_tasks: Tasks that has been visited 'till the circular dependency error
        :return:
        """
        self.tasks_chain = tasks_chain


class Task(object):
    """
    A unit of work to be run by Dagger. Implement the run() method in a subclass to define concrete tasks.

    Task objects must be picklable, and are assumed to be immutable. Changing config or dependencies during execution
    leads to undefined behavior.
    """

    # default number of retries that subclasses can set for their failure mitigation
    DEFAULT_RETRIES = 0

    def __init__(self, config, dependencies=[], retries=None):
        """
        :param config: Picklable data that defines this task's behavior
        :param dependencies: List of tasks that this task depends on
        :param retries: The number of times the task should be retried on failure
        """
        self.config = config
        self.dependencies = dependencies
        self.retries_on_failure = self.DEFAULT_RETRIES if retries is None else retries

    def __str__(self):
        return "{0}".format(
                type(self).__name__
        )

    def get_all_dependencies(self):
        """
        Get a list of all tasks that need to finish for this task to run.
        :return: List of task objects
        """
        all_deps = list(self.dependencies)
        for dep in self.dependencies:
            all_deps += dep.get_all_dependencies()
        return all_deps

    def check_circular_dependencies(self, visiting_list=None):
        """
        This method uses a list passed as parameter to check visited elements
        in the dependencies resolution.
        It allows different threads to run visit on same object without
        influencing each-other dependencies checks.
        If no visiting_list is passed it initializes a new one.

        :param visiting_list: list the list of already visited Tasks.
        :raises: CircularDependencyException
        """

        if visiting_list is None:
            visiting_list = []

        if self in visiting_list:
            raise CircularDependencyException(self)
        else:
            visiting_list.append(self)

        for dep in self.dependencies:
            if dep not in visiting_list:
                dep.check_circular_dependencies(visiting_list)
            else:
                visiting_list.append(dep)
                raise CircularDependencyException(visiting_list)

        visiting_list.remove(self)

    def run(self):
        """
        Implement this task's behavior.

        Note that the implementation should not mutate the task's properties, nor change global variables, as it is run
        in a separate process.
        """
        raise NotImplementedError()
