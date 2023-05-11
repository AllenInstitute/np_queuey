from __future__ import annotations

import contextlib
import json
import pathlib
import random
import shutil
import subprocess
import time
from typing import Generator, NoReturn

import np_logging
import np_session
from huey import MemoryHuey
from np_jobs import (Job, JobT, PipelineQCQueue, SessionArgs, get_job,
                     get_session, update_status)
from typing_extensions import Literal

logger = np_logging.getLogger()

huey = MemoryHuey(immediate=True)

Q = PipelineQCQueue()

@huey.task()
def qc_outstanding_sessions() -> None:
    job: Job | None = Q.next()
    if job is None:
        logger.info('No outstanding sessions in QC queue')
        return
    if Q.is_started(job):
        logger.info('QC already started for %s', job.session)
        return
    run_qc(job)

def run_qc(session_or_job: Job | SessionArgs) -> None:
    job = get_job(session_or_job, Job)
    np_logging.web('np_queuey').info('Starting QC %s', job.session)
    with update_status(Q, job):
        start_qc(job)
    np_logging.web('np_queuey').info('QC finished for %s', job.session)

def start_qc(session_or_job: Job | SessionArgs) -> None:
    session = get_session(session_or_job)
    subprocess.run([
        "C:/Users/svc_neuropix/Documents/GitHub/np_pipeline_qc2/scripts/launch_qc_all_modules.bat",
        session.folder,
    ])
    
def main() -> NoReturn:
    """Run synchronous task loop."""
    while True:
        qc_outstanding_sessions()
        time.sleep(300)
                
if __name__ == '__main__':
    import doctest
    doctest.testmod(verbose=False)
    main()
