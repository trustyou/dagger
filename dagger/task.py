class Task(object):
    """
    A unit of work to be run by Dagger. Implement the run() method in a subclass to define concrete tasks.

    Task objects must be picklable, and are assumed to be immutable. Changing config or dependencies during execution
    leads to undefined behavior.
    """

    def __init__(self, config, dependencies=[]):
        """
        :param config: Picklable data that defines this task's behavior
        :param dependencies: List of tasks that this task depends on
        """
        self.config = config
        self.dependencies = dependencies

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

    def run(self):
        """
        Implement this task's behavior.

        Note that the implementation should not mutate the task's properties, nor change global variables, as it is run
        in a separate process.
        """
        raise NotImplementedError()