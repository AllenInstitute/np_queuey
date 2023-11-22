from __future__ import annotations

import contextlib
import json
import pathlib
import random
import shutil
import subprocess
import time
from typing import Generator, Iterable, NoReturn

import np_logging
import np_session
import np_tools
from huey import MemoryHuey
from np_jobs import (Job, JobT, PipelineNpexpUploadQueue, PipelineQCQueue,
                     PipelineSortingQueue, SessionArgs, SortingJob, get_job,
                     get_session, update_status)
from typing_extensions import Literal

logger = np_logging.getLogger()

huey = MemoryHuey(immediate=True)

SORTING_Q = PipelineSortingQueue()
UPLOAD_Q = PipelineNpexpUploadQueue()

@huey.task()
def sort_outstanding_sessions() -> None:
    job: SortingJob | None = SORTING_Q.next()
    if job is None:
        logger.info('No outstanding sessions to sort')
        return
    if SORTING_Q.is_started(job):
        logger.info('Sorting already started for %s', job.session)
        return
    run_sorting(job)
    add_job_to_pipeline_qc_queue(job)

def run_sorting(session_or_job: SortingJob | SessionArgs) -> None:
    job = get_job(session_or_job, SortingJob)
    np_logging.web('np_queuey').info('Starting sorting %s probes %s', job.session, job.probes)
    with update_status(SORTING_Q, job):
        remove_existing_sorted_folders_on_npexp(job)
        start_sorting(job)
        move_sorted_folders_to_npexp(job)
        remove_raw_data_on_acq_drives(job)
    np_logging.web('np_queuey').info('Sorting finished for %s', job.session)

def probe_folders(session_or_job: SortingJob | SessionArgs) -> tuple[str]:
    job = get_job(session_or_job, SortingJob)
    return tuple(f'{job.session}_probe{probe_letter.upper()}_sorted' for probe_letter in job.probes)

def remove_existing_sorted_folders_on_npexp(session_or_job: SortingJob | SessionArgs) -> None:
    job = get_job(session_or_job, SortingJob)
    for probe_folder in probe_folders(job):
        logger.info('Checking for existing sorted folder %s', probe_folder)
        path = np_session.Session(job.session).npexp_path / probe_folder
        if path.exists():
            logger.info('Removing existing sorted folder %s', probe_folder)
            shutil.rmtree(path.as_posix(), ignore_errors=True)

def start_sorting(session_or_job: SortingJob | SessionArgs) -> None:
    job = get_job(session_or_job, SortingJob)
    path = pathlib.Path('c:/Users/svc_neuropix/Documents/GitHub/ecephys_spike_sorting/ecephys_spike_sorting/scripts/full_probe3X_from_extraction_nopipenv.bat')
    if not path.exists():
        raise FileNotFoundError(path)
    args = [job.session, ''.join(_ for _ in str(job.probes))]
    subprocess.run([str(path), *args])
 
def move_sorted_folders_to_npexp(session_or_job: SortingJob | SessionArgs) -> None:
    """Move the sorted folders to the npexp drive
    Assumes D: processing drive - might want to move this to rig for
    specific-config.
    Cannot robocopy with * for folders, so we must do each probe separately.
    """
    job = get_job(session_or_job, SortingJob)
    for probe_folder in probe_folders(job):
        src = pathlib.Path(f'D:/{probe_folder}')
        dest = np_session.Session(job.session).npexp_path / probe_folder
        logger.info(f'Moving {src} to {dest}')
        subprocess.run([
            'robocopy', f'{src}', f'{dest}',
             '/MOVE', '/E', '/J', '/COPYALL', '/R:0', '/W:0', '/MT:32'
             ], check=False) # return code from robocopy doesn't signal failure      
        if src.exists():
            np_tools.move(src, dest, ignore_errors=True)
            
def remove_raw_data_on_acq_drives(session_or_job: SortingJob | SessionArgs) -> None:
    session = get_session(session_or_job)
    for drive in ('A:', 'B:'):
        paths = []
        for path in pathlib.Path(drive).glob(f'{session}*'):
            npexp_path = session.npexp_path / path.name
            paths.append(npexp_path)
            if hasattr(session, 'lims_path') and session.lims_path is not None:
                paths.append(session.lims_path / path.name)
            for dest in paths:
                if dest is not None and dest.exists() and (
                    np_tools.dir_size_gb(dest) == np_tools.dir_size_gb(path)
                ):
                    break # matching copy found (have to trust it was prev checksummed)
            else:
                logger.info('Copying %r to npexp', path)
                np_tools.copy(path, npexp_path)
            logger.info('Removing %r', path)
            shutil.rmtree(path, ignore_errors=True)

def add_job_to_pipeline_qc_queue(session_or_job: Job | SessionArgs) -> None:
    PipelineQCQueue().add_or_update(session_or_job)


@huey.task()
def upload_outstanding_sessions() -> None:
    job: Job | None = UPLOAD_Q.next()
    if job is None:
        logger.info('No outstanding sessions in npexp_upload queue')
        return
    if UPLOAD_Q.is_started(job):
        logger.info('Upload already started for %s', job.session)
        return
    run_upload(job)

def run_upload(session_or_job: Job | SessionArgs) -> None:
    job = get_job(session_or_job, Job)
    np_logging.web('np_queuey').info('Starting raw data upload to npexp %s', job.session)
    with update_status(UPLOAD_Q, job):
        start_upload(job)
        add_to_pipeline_sorting_queue(job)
    np_logging.web('np_queuey').info('Validated raw data on npexp %s', job.session)

def start_upload(session_or_job: Job | SessionArgs) -> None:
    session = get_session(session_or_job)
    for src in raw_data_folders(session):
        dest = session.npexp_path / src.name
        logger.info(f'Copying {src} to {dest}')
        subprocess.run([
            'robocopy', f'{src}', f'{dest}',
            '/E', '/COPYALL', '/J', '/R:3', '/W:1800', '/MT:32'
            ], check=False) # return code from robocopy doesn't signal failure      
    # checksum validate copies after copying (since exception raised on
    # invalid copies)
    for src in raw_data_folders(session):
        dest = session.npexp_path / src.name
        np_tools.copy(src, dest)
        logger.info(f'Checksum-validated copy of {src} at {dest}')

def raw_data_folders(session_or_job: Job | SessionArgs) -> Iterable[pathlib.Path]:
    session = get_session(session_or_job)
    for drive in ('A:', 'B:'):
        for src in pathlib.Path(drive).glob(f'{session}*'):
            yield src
    
def add_to_pipeline_sorting_queue(session_or_job: Job | SessionArgs) -> None:
    PipelineSortingQueue().add_or_update(session_or_job)


def main() -> NoReturn:
    """Run synchronous task loop."""
    while True:
        upload_outstanding_sessions()
        sort_outstanding_sessions()
        time.sleep(300)
                
if __name__ == '__main__':
    import doctest
    doctest.testmod(verbose=False)
    main()
