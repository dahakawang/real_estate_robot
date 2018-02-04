from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
from common.errors import *
import traceback
import sys
import time


def retry_wrapper(job):
    TOTAL_RETRY = 10
    for retry in range(TOTAL_RETRY):
        try:
            return job.start_sync()
        except HttpFetchError as e:
            error_str = "failed to fetch http data"
            trace_str = traceback.format_exc()
        except KnownError as e:
            print("known exception throwed {} for {}".format(e, job), file=sys.stderr)
            raise
        except:
            print("unknown exception throwed for {}:\n{}".format(job, traceback.format_exc()), file=sys.stderr)
            raise

        if retry != TOTAL_RETRY - 1:
            wait_sec = 10 * (retry + 1)
            print("{} for {}, retrying after {}s... (retried {} times)\n{}".format(error_str, job, wait_sec, retry, trace_str), file=sys.stderr)
            time.sleep(wait_sec)
        else:
            print("{} for {}, tried {} times, exiting...\n{}".format(error_str, job, 1 + retry, trace_str), file=sys.stderr)
    raise MaxExceptionError()


def post_jobs(jobs, concurrency):
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(retry_wrapper, job) for job in jobs]
        wait(futures, timeout=None, return_when=ALL_COMPLETED)
        return len([1 for f in futures if not f.exception()]),\
               len([1 for f in futures if f.exception()]),\
               [r for job in futures if not job.exception() for r in job.result()]


if __name__ == "__main__":
    class Job:
        def __init__(self, exception):
            self.exception = exception

        def __repr__(self):
            return "Job"

        def start_sync(self):
            if self.exception:
                raise self.exception
            return [1]

    r = post_jobs([Job(RuntimeError("let's just play")), Job(RuntimeError()), Job(None), Job(None)], 10)
    print(r)
    # post_jobs([Job(HttpFetchError())], 10)
