# dagger

Dagger performs tasks, providing you with parallel execution, and dependency resolution.

Workflows are defined as graphs (**d**irected **a**cyclic **g**raphs … get it?). You tell dagger to
run certain tasks, and it makes sure that dependencies are discovered and run in a correct order.

Tasks are run in a process pool of configurable size.

You define tasks by subclassing `Task`:

    class DoStuff(Task):
    
        def run(self):
            print("Look at me, I'm runniiiiiing ...")
            
Tasks accept two parameters during creation

* `config`: Something picklable to customize the tasks behavior at runtime
* `dependencies`: A list of `Task` instances that need to be done before we start this task

If task execution fails, a `DaggerException` is raised, with information about which tasks completed
and which failed.

See also [examples folder](dagger/examples).