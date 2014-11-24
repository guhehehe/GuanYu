# -*- encoding: utf-8 -*-

from __future__ import absolute_import, division, print_function, with_statement

from functools import wraps
from multiprocessing import Process


# TODO: Make user be able to implement their own job scheduling strategies.
# TODO: Hammer on the name of the job: work? job? task?
# TODO: Make the current process object reserved argument for worker.
def parallelize(work=[], process=4):
    """Parallelize the decorated function.

    :work: An iterable that contains the work to be assigned to the worker.
    :process: The number of process to be executed in parallel.
    """
    def wrapper(f):
        """The wrapper that wraps the worker to make it run in parallel.

        :f: The worker function to be decorated. This function should have one
            reversed argument as its first argument. This argument will be
            passed in a subset of `work` for each process to work with.
            For example, you can define a worker function like this:
                @guanyu.parallelize(all_work)
                def worker(work):
                    ...
            and call it like this:
                worker()
            when called, the worker function gets passed a subset of
            `all_work`.
        """
        @wraps(f)
        def worker(*args, **kwargs):
            """Parallelized worker.

            This is the parallelized version of the decorated function, when
            run, this function will spawn X subprocesses to execute the logic
            defined in the decorated function, where X equals the number
            specified in `process`.
            """
            try:
                jobs = []
                total = len(work)
                chunck_size = int(total / process) + 1

                for i in range(process):
                    start = i * chunck_size
                    end = start + chunck_size
                    part = work[start:end]
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
