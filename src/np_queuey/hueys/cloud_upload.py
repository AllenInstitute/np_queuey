from __future__ import annotations

import contextlib
import json
import pathlib
import random
import shutil
import subprocess
import time
from typing import Generator, NoReturn

import np_envs
import np_jobs
import np_logging
from huey import MemoryHuey
from typing_extensions import Literal

logger = np_logging.getLogger()

huey = MemoryHuey(immediate=True)

queues = {
    'np_codeocean': np_jobs.CodeOceanUploadQueue(),
    'np_datajoint': np_jobs.DataJointUploadQueue(),
    }
envs = {
    'np_codeocean': np_envs.venv('np_codeocean'),
    'np_datajoint': np_envs.venv('np_datajoint'),
    }
@huey.task()
def process_outstanding_sessions(queue: np_jobs.JobQueue) -> None:
    job: np_jobs.Job | None = queue.next()
    if job is None:
        logger.info(f'No outstanding sessions in {queue.__class__.__name__}')
        return
    if queue.is_started(job):
        logger.info('Job already started for %s', job.session)
        return
    run_job(job, queue)

def run_job(session_or_job: np_jobs.Job | np_jobs.SessionArgs, queue: np_jobs.JobQueue) -> None:
    job = np_jobs.get_job(session_or_job, np_jobs.Job)
    np_logging.web('np_queuey').info('Uploading %r to %s', job.session, queue.__class__.__name__)
    with np_jobs.update_status(queue, job):
        start_job_in_subprocess(job, queue)
    np_logging.web('np_queuey').info('Upload finished for %r to %s', job.session, queue.__class__.__name__)

def start_job_in_subprocess(session_or_job: np_jobs.Job | np_jobs.SessionArgs, queue: np_jobs.JobQueue) -> None:
    session = np_jobs.get_session(session_or_job)
    pkg = next(k for k in queues if queue is queues[k])
    venv = envs[pkg]
    subprocess.run(
        f"{venv.python} -m {pkg}.upload {session.folder}",
        shell=True,
        check=True,
    )
    
def main() -> NoReturn:
    """Run synchronous task loop."""
    for env in envs.values():
        env.update()
    while True:
        for queue in queues.values():
            process_outstanding_sessions(queue)
        time.sleep(300)
                
if __name__ == '__main__':
    import doctest
    doctest.testmod(verbose=False)
    main()
