# -*- encoding: utf-8 -*-

from __future__ import absolute_import, division, print_function, with_statement


from errors import TooManyTasksError
from functools import wraps
from multiprocessing import Process


# TODO: Make the current process object reserved argument for worker?
class GuanYu(object):

    def __init__(self, process=4):
        self.process = process

    def scheduler(self, tasks):
        """This method schedules `tasks` for the processes.

        This method evenly assigns `tasks` to all the workers so that the
        difference of the number of tasks each worker gets assigned is no more
        than 1.

        This method takes a single argument as the tasks for scheduling, and
        should return an iterable of sub tasks to be assigned to each worker.
        Since each worker corresponds to a process, the number of sub tasks
        should be no more than the number of processes.

        You can override this method to define your own scheduling strategy.

        :tasks: A iterable of tasks to be assigned to each worker process.
        """
        parts = []
        total = len(tasks)
        chunck_size = int(total / self.process)
        remainder = total % self.process

        end = 0
        for i in range(self.process):
            start = end
            end = start + chunck_size
            if remainder > 0:
                end += 1
            parts.append(tasks[start:end])
            remainder -= 1

        return parts

    def parallelize(self, tasks=[]):
        """A decorator that parallelizes the decorated function.

        :tasks:   An iterable that contains the tasks to be assigned to the
                  worker.
        :process: The number of process to be executed in parallel.
        """
        def wrapper(f):
            """The wrapper that wraps the worker to make it run in parallel.

            :f: The worker function to be decorated. This function should have
                one reversed argument as its first argument. This argument
                will be passed in a subset of `tasks` for each process to work
                on. For example, you can define a worker function like this:
                    @guanyu.parallelize(all_work)
                    def worker(tasks):
                        ...
                and call it like this:
                    worker()
                when called, the worker function gets passed a subset of
                `all_work`.
            """
            @wraps(f)
            def worker(*args, **kwargs):
                """This function is the parallelized worker.

                This is the parallelized version of the decorated function,
                when run, this function will spawn X subprocesses to execute
                the logic defined in the decorated function, where X equals
                the number specified in `process`.
                """
                try:
                    jobs = []
                    parts = self.scheduler(tasks)
                    if len(parts) > self.process:
                        raise TooManyTasksError()

                    for i in range(self.process):
                        if i > len(parts) - 1:
                            break
                        part = parts[i]
                        if not part:
                            break

                        # Reserve the first argument of the worker function
                        # for the task that should be assigned to it.
                        new_args = [part]
                        new_args.extend(args)
                        p = Process(target=f, args=new_args, kwargs=kwargs)
                        jobs.append(p)
                        p.start()

                    for job in jobs:
                        job.join()
                finally:
                    for job in jobs:
                        job.terminate()
            return worker
        return wrapper
