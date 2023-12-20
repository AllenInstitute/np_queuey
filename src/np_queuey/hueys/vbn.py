from __future__ import annotations

import configparser
import contextlib
import itertools
import json
import os
import pathlib
import random
import re
import shutil
import subprocess
import time
from typing import Generator, Iterable, NoReturn

import boto3
import np_config
import np_logging
import np_session
import np_tools
import upath
from huey import MemoryHuey
from np_jobs import (Job, JobT, PipelineNpexpUploadQueue, SessionArgs,
                     VBNExtractionQueue, VBNUploadQueue, get_job, get_session,
                     update_status)
from typing_extensions import Literal

logger = np_logging.getLogger()

huey = MemoryHuey(immediate=True)

EXTRACTION_Q = VBNExtractionQueue()
UPLOAD_Q = VBNUploadQueue()

AWS_CREDENTIALS: dict[Literal['aws_access_key_id', 'aws_secret_access_key'], str] = np_config.fetch('/projects/vbn_upload')['aws']['credentials']
"""Config for connecting to AWS/S3 via awscli/boto3"""

AWS_CONFIG: dict[Literal['region'], str]  = np_config.fetch('/projects/vbn_upload')['aws']['config']
"""Config for connecting to AWS/S3 via awscli/boto3"""

S3_BUCKET = np_config.fetch('/projects/vbn_upload')['aws']['bucket']
S3_PATH = upath.UPath(f"s3://{S3_BUCKET}") 

SESSION = boto3.session.Session(aws_access_key_id=AWS_CREDENTIALS['aws_access_key_id'], aws_secret_access_key=AWS_CREDENTIALS['aws_secret_access_key'], region_name='us-west-2')

@huey.task()
def extract_outstanding_sessions() -> None:
    job: Job | None = EXTRACTION_Q.next()
    if job is None:
        logger.info('No outstanding sessions to extract')
        return
    if EXTRACTION_Q.is_started(job):
        logger.info('Extraction already started for %s', job.session)
        return
    run_extraction(job)

def run_extraction(session_or_job: Job | SessionArgs) -> None:
    job = get_job(session_or_job, Job)
    np_logging.web('np_queuey-vbn').info('Starting extraction %s', job.session)
    with update_status(EXTRACTION_Q, job):
        #! sorting pipeline will download raw data from lims
        # if not (d := get_local_raw_dirs(job)) or len(d) < 2:
        #     download_raw_data_from_lims(job)
        
        # we'll use extracted + renamed dirs from sorting pipeline
        if not (d := get_local_sorted_dirs(job)) or len(d) < 6:
            if len(d) > 0:
                remove_local_extracted_data(job) # else sorting pipeline won't re-extract
            extract_local_raw_data(job)
        verify_extraction(job)
        upload_extracted_data_to_s3(job)
        np_logging.web('np_queuey-vbn').info('Starting upload for %s', job.session)
        upload_sync_file_to_s3(job)
        remove_local_raw_data(job)
        remove_local_extracted_data(job)
    np_logging.web('np_queuey-vbn').info('Upload finished for %s', job.session)

RAW_DRIVES = ('A:', 'B:', 'C:',)
EXTRACTED_DRIVES = ('C:', 'D:',)

def get_raw_dirs_on_lims(session_or_job: Job | SessionArgs) -> tuple[pathlib.Path, ...]:
    """
    >>> [p.as_posix() for p in get_raw_dirs_on_lims(1051155866)]
    ['//allen/programs/braintv/production/visualbehavior/prod0/specimen_1023232776/ecephys_session_1051155866/1051155866_524760_20200917_probeABC', '//allen/programs/braintv/production/visualbehavior/prod0/specimen_1023232776/ecephys_session_1051155866/1051155866_524760_20200917_probeDEF']
    """
    session = get_session(session_or_job)
    raw_paths = tuple(session.lims_path.glob('*_probe???'))
    assert len(raw_paths) == 2, f'Expected 2 raw paths on lims for {session}, found {len(raw_paths)}'
    return raw_paths

def get_local_raw_dirs(session_or_job: Job | SessionArgs) -> tuple[pathlib.Path, ...]:
    session = get_session(session_or_job)
    paths = []
    for drive in RAW_DRIVES:
        paths.extend(pathlib.Path(drive).glob(f'{session}_probe???'))
    return tuple(paths)

def get_local_extracted_dirs(session_or_job: Job | SessionArgs) -> tuple[pathlib.Path, ...]:
    session = get_session(session_or_job)
    paths = []
    for drive in EXTRACTED_DRIVES:
        p = pathlib.Path(drive)
        paths.extend(p.glob(f'{session}_probe???_extracted'))
    return tuple(paths)

def get_local_sorted_dirs(session_or_job: Job | SessionArgs) -> tuple[pathlib.Path, ...]:
    session = get_session(session_or_job)
    paths = []
    for drive in EXTRACTED_DRIVES:
        p = pathlib.Path(drive)
        paths.extend(p.glob(f'{session}_probe?_sorted'))
    return tuple(paths)

def get_session_upload_path(session_or_job: Job | SessionArgs) -> upath.UPath:
    """
    >>> get_session_upload_path(1051155866).as_posix()
    's3://staging.visual-behavior-neuropixels-data/raw-data/1051155866'
    """
    return S3_PATH / 'raw-data' / str(get_session(session_or_job).lims.id)

def get_sync_file(session_or_job: Job | SessionArgs) -> pathlib.Path:
    return pathlib.Path(
        get_session(session_or_job).data_dict['sync_file']
    )

    
def get_probe_id(session_or_job: Job | SessionArgs, path: str | pathlib.Path) -> int:
    """
    >>> get_probe_id(1051155866, 'A:/Neuropix-PXI-slot2-probe2-AP')
    1051284113
    >>> get_probe_id(1051155866, 'A:/1051155866_524760_20200917_probeB')
    1051284113
    >>> get_probe_id(1051155866, 'D:/1051155866_524760_20200917_probeB_sorted/continuous.dat')
    1051284113
    """
    if 'slot' in str(path):
        # extract slot and port
        match = re.search(r"slot(?P<slot>[0-9]{1})-probe(?P<port>[0-9]{1})", str(path))
        assert match is not None, f'Could not find slot and probe ints in {path}'
        slot = match.group("slot")
        port = match.group("port")
    else:
        # extract slot and port
        match = re.search(r"[pP]robe(?P<probe>[A-F]{1})(?![A-F])", str(path))
        assert match is not None, f'Could not find probe letter in {path}'
        probe_letter = match.group("probe")
        port = str(('ABC' if probe_letter in 'ABC' else 'DEF').index(probe_letter) + 1) 
        slot = '2' if probe_letter in 'ABC' else '3'
    session = get_session(session_or_job)
    probes = session.lims['ecephys_probes']
    for p in probes:
        info = p['probe_info']['probe']
        if (info['slot'], info['port']) == (slot, port):
            break
    else:
        raise ValueError(f'Could not find probe {slot=}-{port=} for {path} in LIMS for {session}')
    return p['id']

def get_dest_from_src(session_or_job: Job | SessionArgs, src: pathlib.Path) -> upath.UPath | None:
    """
    >>> get_dest_from_src(1051155866, pathlib.Path('D:/1051155866_524760_20200917_probeB_sorted/Neuropix-PXI-100.0/continuous.dat')).as_posix()
    's3://staging.visual-behavior-neuropixels-data/raw-data/1051155866/1051284113/spike_band.dat'
    """
    
    try:
        probe_id = get_probe_id(session_or_job, src)
    except AssertionError:
        probe_id = None
    if probe_id:
        is_lfp = 'lfp' in src.as_posix().lower() or src.parent.name.endswith('.1')
        if src.name == 'continuous.dat':
            name = 'lfp_band.dat' if is_lfp else 'spike_band.dat' 
        elif src.name in ('event_timestamps.npy', 'channel_states.npy'):
            name = src.name
        else:
            return None
        dest = get_session_upload_path(session_or_job) / f'{probe_id}' / name
    else:
        if src.suffix in ('.sync', '.h5'):
            name = 'sync.h5'
            dest = get_session_upload_path(session_or_job) / name
        else:
            return None
    return dest


def assert_s3_path_exists() -> None:
    if not S3_PATH.exists():
        raise FileNotFoundError(f'{S3_PATH} does not exist')
    logger.info(f'Found {S3_PATH}')
    
def assert_s3_read_access() -> None:
    assert_s3_path_exists()
    # _ = tuple(S3_PATH.iterdir())
    # logger.info(f'Found {len(_)} objects in {S3_PATH}')
    
def assert_s3_write_access() -> None:
    test = S3_PATH / 'test' / 'test.txt'
    try:
        test.write_text("test")
    except PermissionError as e:
        raise PermissionError(f'Could not write to {S3_PATH}') from e
    logger.info(f'Wrote {test}')
    try:
        test.unlink()
    except PermissionError as e:
        raise PermissionError(f'Could not delete from {S3_PATH}') from e
    logger.info(f'Deleted {test}')
    
def download_raw_data_from_lims(session_or_job: Job | SessionArgs) -> None:
    session = get_session(session_or_job)
    raw_paths = get_raw_dirs_on_lims(session)
    for (drive, src) in zip(('A:', 'B:'), raw_paths):
        dest = pathlib.Path(f'{drive}/{src.name}')
        logger.info(f'Copying {src} to {dest}')
        np_tools.copy(src, dest)
    logger.info('Finished copying raw data from lims')

def verify_extraction(session_or_job: Job | SessionArgs) -> None:
    session = get_session(session_or_job)
    raw_paths = get_raw_dirs_on_lims(session)
    extracted_paths = get_local_extracted_dirs(session)
    assert len(extracted_paths) == 2, f'Expected 2 extracted dirs, found {len(extracted_paths)}'
    sorted_paths = get_local_sorted_dirs(session)
    assert len(sorted_paths) == 6, f'Expected 6 renamed-sorted dirs, found {len(sorted_paths)}'
    raw_size = sum(np_tools.dir_size_gb(p) for p in raw_paths)
    extracted_size = sum(np_tools.dir_size_gb(p) for p in sorted_paths)
    if raw_size > extracted_size:
        raise ValueError(f'Extraction failed for {session}: total size of raw folders is bigger than extracted folders')
    logger.info('Finished verifying extraction')


def remove_local_extracted_data(session_or_job: Job | SessionArgs) -> None:
    session = get_session(session_or_job)
    paths = []
    paths.extend(get_local_extracted_dirs(session))
    paths.extend(get_local_sorted_dirs(session))
    for path in paths:
        logger.info(f'Removing {path}')
        shutil.rmtree(path.as_posix(), ignore_errors=True)
    logger.info('Finished removing local extracted data')

def remove_local_raw_data(session_or_job: Job | SessionArgs) -> None:
    for path in get_local_raw_dirs(get_session(session_or_job)):
        logger.info(f'Removing {path}')
        shutil.rmtree(path.as_posix(), ignore_errors=True)
    logger.info('Finished removing local raw data')

    
def extract_local_raw_data(session_or_job: Job | SessionArgs) -> None:
    job = get_job(session_or_job, Job)
    path = pathlib.Path('c:/Users/svc_neuropix/Documents/GitHub/ecephys_spike_sorting/ecephys_spike_sorting/scripts/just_extraction.bat')
    if not path.exists():
        raise FileNotFoundError(path)
    args = [job.session]
    subprocess.run([str(path), *args])
    logger.info('Finished extracting raw data')
    
def upload_extracted_data_to_s3(session_or_job: Job | SessionArgs) -> None:
    dirs = get_local_sorted_dirs(session_or_job)
    if len(dirs) > 6:
        raise AssertionError(f'Expected 2 extracted, renamed sorted dirs, found {len(dirs)}')
    for parent in dirs:
        for subpath in parent.rglob('*'):
            if subpath.is_dir():
                continue
            dest = get_dest_from_src(session_or_job, subpath)
            if dest is None:
                continue
            upload_file(subpath, dest)
    
def upload_file(src: pathlib.Path, dest: pathlib.Path) -> None:
    client = SESSION.client("s3")
    logger.info(f'Uploading {src} -> {dest}')
    client.upload_file(src, S3_BUCKET, dest.as_posix().split(S3_PATH.as_posix())[-1]) # relative_to doesn't work
     
def get_s3_key(path: upath.UPath) -> str:
    """
    >>> p = 's3://staging.visual-behavior-neuropixels-data/raw-data/1051155866/1051284112/lfp_band.dat'
    >>> get_s3_key(p)
    'raw-data/1051155866/1051284112/lfp_band.dat'
    >>> get_s3_key(upath.UPath(p))
    'raw-data/1051155866/1051284112/lfp_band.dat'
    >>> assert get_s3_key(p) == get_s3_key(upath.UPath(p))
    """
    if isinstance(path, upath.UPath):
        path = path.as_posix()
    if isinstance(path, pathlib.Path):
        raise TypeError(f'get_s3_key expects a string or upath.UPath: s3 URI is not encoded correctly in pathlib.Path')
    assert isinstance(path, str)
    return path.split(S3_PATH.as_posix())[-1]
          
def upload_sync_file_to_s3(session_or_job: Job | SessionArgs) -> None:
    sync = get_sync_file(session_or_job)
    dest = get_dest_from_src(session_or_job, sync)
    assert dest is not None, f'Could not find dest for {sync}'
    upload_file(sync, dest)
            
def get_home_dir() -> pathlib.Path:
    if os.name == 'nt':
        return pathlib.Path(os.environ['USERPROFILE'])
    return pathlib.Path(os.environ['HOME'])

def get_aws_files() -> dict[Literal['config', 'credentials'], pathlib.Path]:
    return {
        'config': get_home_dir() / '.aws' / 'config',
        'credentials': get_home_dir() / '.aws' / 'credentials',
    }
    
def verify_ini_config(path: pathlib.Path, contents: dict, profile: str = 'default') -> None:
    config = configparser.ConfigParser()
    if path.exists():
        config.read(path)
    if not all(k in config[profile] for k in contents):
        raise ValueError(f'Profile {profile} in {path} exists but is missing some keys required for s3 access.')
    
def write_or_verify_ini_config(path: pathlib.Path, contents: dict, profile: str = 'default') -> None:
    config = configparser.ConfigParser()
    if path.exists():
        config.read(path)
        try:    
            verify_ini_config(path, contents, profile)
        except ValueError:
            pass
        else:   
            return
    config[profile] = contents
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch(exist_ok=True)
    with path.open('w') as f:
        config.write(f)
    verify_ini_config(path, contents, profile)

def verify_json_config(path: pathlib.Path, contents: dict) -> None:
    config = json.loads(path.read_text())
    if not all(k in config for k in contents):
        raise ValueError(f'{path} exists but is missing some keys required for codeocean or s3 access.')
    
def write_or_verify_json_config(path: pathlib.Path, contents: dict) -> None:
    if path.exists():
        try:
            verify_json_config(path, contents)
        except ValueError:
            contents = np_config.merge(json.loads(path.read_text()), contents)
        else:   
            return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch(exist_ok=True)
    path.write_text(json.dumps(contents, indent=4))
    
    
def ensure_credentials() -> None:
    for file, contents in (
        (get_aws_files()['config'], AWS_CONFIG),
        (get_aws_files()['credentials'], AWS_CREDENTIALS),
    ):
        assert isinstance(contents, dict)
        write_or_verify_ini_config(file, contents, profile='default')
        logger.info('Wrote %s', file)
        

def add_job_to_upload_queue(session_or_job: Job | SessionArgs) -> None:
    UPLOAD_Q.add_or_update(session_or_job)


def main() -> NoReturn:
    """Run synchronous task loop."""
    while True:
        extract_outstanding_sessions()
        time.sleep(300)
                
                
if __name__ == '__main__':
    import doctest
    doctest.testmod()
    ensure_credentials()
    assert_s3_read_access()
    assert_s3_write_access()
    main()