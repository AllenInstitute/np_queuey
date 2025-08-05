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

SESSION_IDS = [
    "1051155866_524760_20200917",
    "1051155866_524760_20200917",
    "1051155866_524760_20200917",
    "1044385384_524761_20200819",
    "1044594870_524761_20200820",
    "1056495334_531237_20201014",
    "1056720277_531237_20201015",
    "1055221968_533537_20201007",
    "1055403683_533537_20201008",
    "1055240613_533539_20201007",
    "1055415082_533539_20201008",
    "1062755779_541234_20201111",
    "1063010385_541234_20201112",
    "1062755416_542072_20201111",
    "1063010496_542072_20201112",
    "1070961372_544835_20201216",
    "1067588044_544836_20201202",
    "1067781390_544836_20201203",
    "1069192277_544837_20201209",
    "1069458330_544837_20201210",
    "1065449881_544838_20201123",
    "1065908084_544838_20201124",
    "1071300149_548465_20201217",
    "1077712208_548715_20210120",
    "1084427055_548716_20210217",
    "1079018622_548717_20210127",
    "1079278078_548717_20210128",
    "1081079981_548720_20210203",
    "1081429294_548720_20210204",
    "1081090969_548721_20210203",
    "1081431006_548721_20210204",
    "1099598937_560962_20210428",
    "1099869737_560962_20210429",
    "1104052767_560964_20210519",
    "1104297538_560964_20210520",
    "1109680280_568022_20210616",
    "1109889304_568022_20210617",
    "1108335514_571520_20210609",
    "1108528422_571520_20210610",
    "1116941914_576323_20210721",
    "1117148442_576323_20210722",
    "1118324999_576324_20210728",
    "1118512505_576324_20210729",
    "1119946360_578003_20210804",
    "1120251466_578003_20210805",
    "1130113579_579993_20210922",
    "1130349290_579993_20210923",
    "1128520325_585326_20210915",
    "1128719842_585326_20210916",
    "1139846596_585329_20211110",
    "1140102579_585329_20211111",
    "1047969464_509808_20200902",
    "1048189115_509808_20200903",
    "1047977240_524925_20200902",
    "1048196054_524925_20200903",
    "1052331749_524926_20200923",
    "1052530003_524926_20200924",
    "1046166369_527294_20200826",
    "1046581736_527294_20200827",
    "1053718935_527749_20200930",
    "1053941483_527749_20201001",
    "1065437523_544358_20201123",
    "1065905010_544358_20201124",
    "1064400234_544456_20201118",
    "1064644573_544456_20201119",
    "1069193611_545994_20201209",
    "1069461581_545994_20201210",
    "1077711823_545996_20210120",
    "1077897245_545996_20210121",
    "1084428217_550324_20210217",
    "1084939136_550324_20210218",
    "1086200042_554013_20210224",
    "1086410738_554013_20210225",
    "1087720624_556014_20210303",
    "1087992708_556014_20210304",
    "1089296550_556016_20210310",
    "1086198651_556021_20210224",
    "1086433081_556021_20210225",
    "1095138995_558306_20210407",
    "1095340643_558306_20210408",
    "1093638203_560356_20210331",
    "1093867806_560356_20210401",
    "1098119201_563323_20210421",
    "1101263832_563326_20210505",
    "1106985031_563495_20210602",
    "1107172157_563495_20210603",
    "1104058216_563497_20210519",
    "1104289498_563497_20210520",
    "1101268690_564012_20210505",
    "1101473342_564012_20210506",
    "1105543760_567286_20210526",
    "1105798776_567286_20210527",
    "1115077618_570299_20210713",
    "1115356973_570299_20210714",
    "1108334384_570301_20210609",
    "1108531612_570301_20210610",
    "1122903357_570302_20210818",
    "1123100019_570302_20210819",
    "1112302803_572846_20210630",
    "1112515874_572846_20210701",
    "1115086689_574078_20210713",
    "1115368723_574078_20210714",
    "1121406444_574081_20210811",
    "1121607504_574081_20210812",
    "1118327332_574082_20210728",
    "1118508667_574082_20210729",
    "1124285719_577287_20210825",
    "1124507277_577287_20210826",
    "1125713722_578257_20210901",
    "1125937457_578257_20210902",
    "1043752325_506940_20200817",
    "1044016459_506940_20200818",
    "1044389060_510589_20200819",
    "1044597824_510589_20200820",
    "1049273528_521466_20200909",
    "1049514117_521466_20200910",
    "1052342277_530862_20200923",
    "1052533639_530862_20200924",
    "1053709239_532246_20200930",
    "1053925378_532246_20201001",
    "1059678195_536211_20201028",
    "1059908979_536211_20201029",
    "1061238668_536213_20201104",
    "1061463555_536213_20201105",
    "1064415305_536480_20201118",
    "1064639378_536480_20201119",
    "1072345110_540536_20201222",
    "1072572100_540536_20201223",
    "1076265417_546503_20210113",
    "1076487758_546503_20210114",
    "1072341440_546507_20201222",
    "1072567062_546507_20201223",
    "1079018673_546508_20210127",
    "1079275221_546508_20210128",
    "1067790400_546512_20201203",
    "1093642839_553253_20210331",
    "1093864136_553253_20210401",
    "1090803859_553960_20210317",
    "1091039376_553960_20210318",
    "1099596266_553963_20210428",
    "1099872628_553963_20210429",
    "1087723305_553964_20210303",
    "1087993643_553964_20210304",
    "1090800639_555304_20210317",
    "1091039902_555304_20210318",
    "1096620314_560770_20210414",
    "1096935816_560770_20210415",
    "1092283837_560771_20210324",
    "1092466205_560771_20210325",
    "1113751921_562033_20210707",
    "1113957627_562033_20210708",
    "1111013640_568963_20210623",
    "1111216934_568963_20210624",
    "1152632711_599294_20220119",
    "1152811536_599294_20220120",
]
FOR_SCALING = [s for s in SESSION_IDS if any(s.startswith(str(x)) for x in [1043752325, 1044016459, 1044385384, 1044389060, 1044594870,       1044597824, 1046166369, 1046581736, 1047969464, 1047977240,       1048189115, 1048196054, 1049273528, 1049514117, 1051155866,       1052331749, 1052342277, 1052530003, 1052533639, 1053709239,       1053718935, 1053925378, 1053941483, 1055221968, 1055240613,       1055403683, 1055415082, 1056495334, 1056720277, 1059908979,       1061238668, 1061463555, 1062755416, 1062755779, 1063010385,       1063010496, 1064400234, 1064415305, 1064639378, 1064644573,       1065437523, 1065449881, 1065905010, 1065908084, 1067588044,       1067781390, 1067790400, 1069192277, 1069193611, 1069458330,       1069461581, 1070961372, 1071300149, 1072341440, 1072345110,       1072567062, 1072572100, 1076265417, 1076487758, 1077711823,       1077712208, 1077897245, 1079018622, 1079018673, 1079275221,       1079278078, 1081079981, 1081090969, 1081429294, 1081431006,       1084427055, 1084428217, 1084939136, 1086198651, 1086200042,       1086410738, 1086433081, 1087720624, 1087723305, 1087992708,       1087993643, 1089296550, 1090800639, 1090803859, 1091039376,       1091039902, 1092283837, 1092466205, 1093638203, 1093642839,       1093864136, 1093867806, 1095138995, 1095340643, 1096620314,       1096935816, 1098119201, 1099596266, 1099598937, 1099869737,       1099872628, 1101263832, 1101268690, 1101473342, 1104052767,       1104058216, 1104289498, 1104297538, 1105543760, 1105798776,       1106985031, 1107172157, 1108335514, 1108528422, 1115086689,       1115368723, 1116941914, 1117148442, 1118327332, 1118508667,       1119946360, 1120251466, 1121406444, 1121607504, 1122903357,       1123100019, 1124285719, 1124507277, 1125713722, 1125937457,       1128520325, 1128719842, 1130113579, 1130349290, 1139846596,       1140102579, 1152632711, 1152811536])]
EXTRACTION_Q = VBNExtractionQueue()
UPLOAD_Q = VBNUploadQueue()

AWS_CREDENTIALS: dict[Literal['aws_access_key_id', 'aws_secret_access_key'], str] = np_config.fetch('/projects/vbn_upload')['aws']['credentials']
"""Config for connecting to AWS/S3 via awscli/boto3"""

AWS_CONFIG: dict[Literal['region'], str]  = np_config.fetch('/projects/vbn_upload')['aws']['config']
"""Config for connecting to AWS/S3 via awscli/boto3"""

client_kwargs= dict(aws_access_key_id=AWS_CREDENTIALS['aws_access_key_id'], aws_secret_access_key=AWS_CREDENTIALS['aws_secret_access_key'], region_name='us-west-2')
SESSION = boto3.session.Session(**client_kwargs)

S3_BUCKET = np_config.fetch('/projects/vbn_upload')['aws']['bucket']
S3_PATH = upath.UPath(f"s3://{S3_BUCKET}/visual-behavior-neuropixels", client_kwargs=client_kwargs) 


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
        # sorting pipeline will download raw data from lims, we don't need to do it here
        
        # we'll use extracted + renamed dirs from sorting pipeline
        if not (d := get_local_sorted_dirs(job)) or len(d) < 6:
            if len(d) > 0:
                remove_local_extracted_data(job) # else sorting pipeline won't re-extract
            extract_local_raw_data(job)
        verify_extraction(job)
        replace_timestamps(job)
        update_voltage_conversion_in_settings_xml(job)
        write_extra_probe_metadata(job)
        upload_extracted_data_to_s3(job)
        np_logging.web('np_queuey-vbn').info('Starting upload for %s', job.session)
        # upload_sync_file_to_s3(job) 
        remove_local_raw_data(job)
        remove_local_extracted_data(job)
    np_logging.web('np_queuey-vbn').info('Upload finished for %s', job.session)

RAW_DRIVES = ('A:', 'B:', 'C:',)
EXTRACTED_DRIVES = ('C:', 'D:',)

def needs_scaling(session_or_job: Job | SessionArgs) -> bool:
    """Check if the session needs scaling based on its ID.
    
    >>> needs_scaling(1051155866)
    True
    >>> needs_scaling(1051155866_524760_20200917)
    True
    >>> needs_scaling(1051155866_524760_20200918)
    False
    """
    session = get_session(session_or_job)
    return session.npexp_path.name in FOR_SCALING

def update_voltage_conversion_in_settings_xml(session_or_job: Job | SessionArgs) -> None:
    session = get_session(session_or_job)
    if not needs_scaling(session):
        logger.info(f"No voltage conversion needed for {session.npexp_path.name}")
        return None
    orig = "0.19499999284744262695"
    new = "0.09749999642372131347"
    for extracted_dir in get_local_sorted_dirs(session_or_job):
        for settings_xml in extracted_dir.rglob('settings*.xml'):
            logger.info(f'Updating voltage conversion factor in {settings_xml}')
            settings_xml.write_text(
                settings_xml.read_text().replace(orig, new)
            )

def write_extra_probe_metadata(session_or_job: Job | SessionArgs) -> None:
    all_probe_metadata = get_probe_metadata(session_or_job)
    for extracted_dir in get_local_sorted_dirs(session_or_job):
        metadata = next(d for d in all_probe_metadata
            if any(
                str(v).lower() in extracted_dir.name.lower() 
                for k,v in d.items()
                if k in ('name', 'serial_number',)
           )
        )
        logger.info(f'Writing probe metadata for {extracted_dir.name}: {metadata}')
        (extracted_dir / 'probe_info.json').write_text(json.dumps(metadata, indent=4))
        
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

def get_probe_metadata(session_or_job: Job | SessionArgs) -> list[dict[str, int]]:
    """
    >>> get_surface_channels(1077891954)[0]
    {'name': 'probeF', 'surface_channel': 366, 'serial_number': 1077995099, 'scaling_factor': 1.0}
    """
    session = get_session(session_or_job)
    lfp_subsampling_paths = sorted(session.lims_path.glob('EcephysLfpSubsamplingStrategy/*/ECEPHYS_LFP_SUBSAMPLING_QUEUE_*_input.json'))
    if not lfp_subsampling_paths:
        raise ValueError(f'No LFP subsampling strategy input json found for {session}: cannot determine surface channels')
    data = json.loads(lfp_subsampling_paths[-1].read_text())
    return [
        {
            'name': p['name'], 
            'surface_channel': int(p['surface_channel']),
            'serial_number': get_probe_id(session_or_job, p['name']),
            'scaling_factor': 0.5 if needs_scaling(session_or_job) else 1.0,
        }
        for p in data['probes']
    ]

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

def replace_timestamps(session_or_job: Job | SessionArgs) -> None:
    session = get_session(session_or_job)
    extracted_paths = get_local_sorted_dirs(session)
    recording_dirs = itertools.chain.from_iterable(
        extracted_path.glob("Record Node */experiment1/recording*")
        for extracted_path in extracted_paths
    )
    for timing in npc_ephys.get_ephys_timing_on_sync(
        sync=get_sync_file(session_or_job),
        recording_dirs=recording_dirs,
    ):
        logger.info(f'Replacing timestamps for {timing.device.name}')
        npc_ephys.overwrite_timestamps(timing)
    
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