from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED


# TODO retry after exception
def post_jobs(jobs, concurrency):
    jobs[0].count = 0
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(job.start_sync) for job in jobs]
        wait(futures, timeout=None, return_when=ALL_COMPLETED)