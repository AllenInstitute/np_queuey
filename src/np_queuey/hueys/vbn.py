from __future__ import annotations

import configparser
from dataclasses import dataclass
import dataclasses
import json
import os
import pathlib
import random
import re
import shutil
import subprocess
import time
from typing import Generator, Iterable, NoReturn, Literal
from collections.abc import Sequence
import random
import pathlib


import lazynwb
import polars as pl
import matplotlib.pyplot as plt
import numpy as np
import numpy.typing as npt
import boto3
import np_config
import np_logging
import np_session
import np_tools
from npc_sync import SyncDataset
import upath
from huey import MemoryHuey
from np_jobs import (Job, JobT, PipelineNpexpUploadQueue, SessionArgs,
                     VBNExtractionQueue, VBNUploadQueue, get_job, get_session,
                     update_status)
from typing_extensions import Literal

# disable interactive plot mode which errors in huey
plt.switch_backend('agg')

logger = np_logging.getLogger()

huey = MemoryHuey(immediate=True)

SESSION_IDS = [
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

AWS_CREDENTIALS: dict[Literal['aws_access_key_id', 'aws_secret_access_key'], str] = np_config.fetch('/projects/vbn_upload')['aws']['credentials']
"""Config for connecting to AWS/S3 via awscli/boto3"""

AWS_CONFIG: dict[Literal['region'], str]  = np_config.fetch('/projects/vbn_upload')['aws']['config']
"""Config for connecting to AWS/S3 via awscli/boto3"""

client_kwargs= dict(aws_access_key_id=AWS_CREDENTIALS['aws_access_key_id'], aws_secret_access_key=AWS_CREDENTIALS['aws_secret_access_key'], region_name='us-west-2')
SESSION = boto3.session.Session(**client_kwargs)

S3_BUCKET = np_config.fetch('/projects/vbn_upload')['aws']['bucket']
S3_PATH = upath.UPath(f"s3://{S3_BUCKET}/visual-behavior-neuropixels", client_kwargs=client_kwargs) 
assert S3_PATH, f'{S3_PATH=} must be a path'

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
        try:
            extract_local_raw_data(job)
            verify_extraction(job)
            replace_timestamps(job)
            write_qc_plots(job)
            write_extra_probe_metadata(job)
            upload_corrected_settings_xml_file(job)
            upload_extracted_data_to_s3(job)
            np_logging.web('np_queuey-vbn').info('Starting upload for %s', job.session)
        finally:
            # logger.warning(f"Raw data not being removed for the purpose of testing - remember to re-enable")
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

def get_settings_xml_path(session_or_job: Job | SessionArgs) -> pathlib.Path:
    for raw_dir in get_local_raw_dirs(session_or_job):
        if (xml := next(raw_dir.glob('settings*.xml'), None)):
            return xml
    else:
        raise FileNotFoundError(f'No settings.xml file found for {session_or_job}')
    
def upload_corrected_settings_xml_file(session_or_job: Job | SessionArgs) -> None:
    src = update_gain_in_settings_xml(session_or_job)
    dest = get_dest_from_src(session_or_job, src)
    assert dest is not None
    upload_file(src, dest)
    
def update_gain_in_settings_xml(session_or_job: Job | SessionArgs) -> pathlib.Path:
    session = get_session(session_or_job)
    path = get_settings_xml_path(session_or_job)
    if not needs_scaling(session):
        logger.info(f"No voltage conversion needed for {session.npexp_path.name}")
        return path
    xml = path.read_text()
    orig = "0.19499999284744262695"
    new = "0.09749999642372131347"
    if new in xml:
        logger.info(f"Voltage conversion has already been applied to settings.xml for {session.npexp_path.name}")
        return path
    assert orig in xml, f"{path} is uncorrected and has a different gain setting than expected"
    logger.info(f"Updating gain in {path}")
    path.write_text(
        xml.replace(orig, new)
    )
    return path
    
PROBE_METADATA_FILENAME = 'probe_info.json'
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
        (extracted_dir / PROBE_METADATA_FILENAME).write_text(json.dumps(metadata, indent=4))
        
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

def get_probe_metadata(session_or_job: Job | SessionArgs) -> list[dict]:
    """
    >>> get_surface_channels(1077891954)[0]
    {'name': 'probeF', 'surface_channel': 366, 'serial_number': 1077995099, 'scaling_factor': 1.0}
    """
    session = get_session(session_or_job)
    lfp_subsampling_paths = sorted(session.lims_path.glob('EcephysLfpSubsamplingStrategy/*/ECEPHYS_LFP_SUBSAMPLING_QUEUE_*_input.json'))
    if not lfp_subsampling_paths:
        raise ValueError(f'No LFP subsampling strategy input json found for {session}: cannot determine surface channels')
    probe_metadata = json.loads(lfp_subsampling_paths[-1].read_text())
    return [
        {
            'name': p['name'], 
            'surface_channel': int(p['surface_channel']),
            'serial_number': get_probe_id(session_or_job, p['name']),
            'amplitude_scaling_factor': 0.5 if needs_scaling(session_or_job) else 1.0,
            'note': "'amplitude_scaling_factor' applies to data in ap_continuous and lfp_continuous .dat files; the scaling factor as already been applied to the 'gain' parameter in the settings.xml file"
        }
        for p in probe_metadata['probes']
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
    if src.suffix in ('.sync', '.h5'):
        name = 'sync.h5'
        return get_session_upload_path(session_or_job) / name
    if src.suffix == '.xml' and src.stem.startswith('settings'):
        return get_session_upload_path(session_or_job) / 'settings.xml'
    
    probe_id = get_probe_id(session_or_job, src)
    is_lfp = 'lfp' in src.as_posix().lower() or src.parent.name.endswith('.1')
    if src.name == 'continuous.dat':
        name = 'lfp_band.dat' if is_lfp else 'spike_band.dat' 
    elif src.name in (
        # 'event_timestamps.npy', 'channel_states.npy',  
        #! ^^ we've corrected continuous timestamps so these are no longer informative
        'ap_timestamps.npy', 'lfp_timestamps.npy',
        PROBE_METADATA_FILENAME,
    ):
        name = src.name
    else:
        return None
    return get_session_upload_path(session_or_job) / f'{probe_id}' / name


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

TIMESTAMPS_REPLACED_FLAG = 'TIMESTAMPS_CORRECTED'
def replace_timestamps(session_or_job: Job | SessionArgs) -> None:
    for timing in get_start_times_and_sampling_rates(session_or_job):
        flag = timing.timestamps_path.with_name(TIMESTAMPS_REPLACED_FLAG)
        if flag.exists():
            logger.info(f'Timestamps already replaced for {timing.device_name}')
            continue
        timing.timestamps_path.with_name('timing_info.json').write_text(
            json.dumps(
                {
                    'sampling_rate': timing.sampling_rate,
                    'start_time': timing.start_time,
                },
                indent=4,
            )
        )
        flag.touch()
        logger.info(f'Replacing timestamps for {timing.device_name}: {timing.sampling_rate=}')
        first_sample_index = np.load(timing.timestamps_path, mmap_mode='r')[0].item()
        default_continuous = np.arange(
            start=first_sample_index,
            stop=first_sample_index + timing.num_samples ,
            dtype=np.float64,
        )
        adjusted_continuous = (
            default_continuous / timing.sampling_rate
        ) + timing.start_time
        np.save(timing.timestamps_path, adjusted_continuous)

def write_qc_plots(session_or_job) -> None:
    qc_dir = pathlib.Path('//allen/programs/mindscope/workgroups/np-exp/vbn_data_release/raw_data_upload_qc')
    qc_dir.mkdir(exist_ok=True, parents=True)
    for extracted_dir in get_local_sorted_dirs(session_or_job):
        session_id, subject_id, date, probe_name, *_ = extracted_dir.name.split('_')
        ephys_nwb_path = next(
            p for p in pathlib.Path("//allen/programs/mindscope/workgroups/np-exp/vbn_data_release/visual-behavior-neuropixels-0.5.0/behavior_ecephys_sessions").glob("*/ecephys_session_??????????.nwb")
            if session_id in p.as_posix()
        )
        electrodes = lazynwb.scan_nwb(ephys_nwb_path, "general/extracellular_ephys/electrodes", infer_schema_length=1).filter(pl.col("group_name") == probe_name)
        units = lazynwb.scan_nwb(ephys_nwb_path, 'units', infer_schema_length=1)

        probe_units = (
            units
            .filter(
                pl.col('quality') == 'good',
                
            )
            .select('_table_index', 'peak_channel_id', 'amplitude')
            .join(electrodes, left_on='peak_channel_id', right_on='id')
            .collect()
        )   
        if len(probe_units) == 0:
            logger.warning(f'no units in NWB for {session_or_job} {probe_name}')
            continue
        largest_unit = probe_units.sort(pl.col('amplitude').abs())[-1]
        unit = largest_unit
        peak_channel = unit['probe_channel_number'][0]
        spike_times = (
            units
            .filter(pl.col('_table_index') == unit['_table_index'][0])
            .select('spike_times')
            .collect()
        )['spike_times'][0].to_numpy()
        ap_data = np.memmap(extracted_dir / "continuous" / "Neuropix-PXI-100.0" / "continuous.dat", dtype=np.int16).reshape((-1, 384))
        ap_timestamps = np.load(extracted_dir / "continuous" / "Neuropix-PXI-100.0" / "ap_timestamps.npy")
                
        dur = 0.0005

        channel_span = 10
        y_min, y_max = peak_channel - channel_span, peak_channel + channel_span
        y_slice = slice(y_max, y_min, -1)
        sample = random.sample(spike_times.tolist(), k=1)
        for t in sample:
            t_mask = (ap_timestamps >= t - dur) & (ap_timestamps < t + dur)
            data = ap_data[t_mask, y_slice].T
            data = data - np.median(data, axis=0)
            plt.ioff() 
            plt.imshow(data, aspect='auto', cmap='bwr', extent=[-dur, dur, y_min, y_max])
            plt.axvline(0, color='grey', lw=.5)
            plt.colorbar()
            plt.savefig((qc_dir / f"{extracted_dir.name}.png").as_posix())
            plt.close()
        ap_timestamps = None
    
    
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
    object_name = dest.as_posix().split(S3_BUCKET)[-1].strip('/') # relative_to doesn't work
    client.upload_file(src, S3_BUCKET, object_name) 
     
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
        


def main() -> NoReturn:
    """Run synchronous task loop."""
    while True:
        extract_outstanding_sessions()
        time.sleep(300)
                
# sync probe stuff ------------------------------------------------- #
@dataclasses.dataclass
class ProbeTiming:
    device_name: str
    start_time: float
    sampling_rate: float
    timestamps_path: pathlib.Path
    num_samples: int

def get_timestamps_path(session_or_job, probe_name: str, band: str) -> pathlib.Path:
    if band.lower() == 'ap':
        record_node = 'Neuropix-PXI-100.0'
        file_name = 'ap_timestamps.npy'
    elif band.lower() == 'lfp':
        record_node = 'Neuropix-PXI-100.1'
        file_name = 'lfp_timestamps.npy'
    else:
        raise ValueError(f'band must be ap or lfp (case-insensitive): got {band}')
    for extracted_dir in get_local_sorted_dirs(session_or_job):
        if probe_name in extracted_dir.as_posix():
            return extracted_dir / 'continuous' / record_node / file_name
    raise FileNotFoundError(f"timestamps not found for {session_or_job} {probe_name} {band}")

def get_start_times_and_sampling_rates(
    session_or_job: Job | SessionArgs,
) -> tuple[ProbeTiming, ...]:
    results = []
    for probe in np_session.Session(session_or_job).lims['ecephys_probes']:
        for band in ('_', '_lfp_'):
            results.append(
                ProbeTiming(
                    device_name=probe['name'],
                    start_time=-probe['total_time_shift'],
                    sampling_rate=probe[f"global_probe{band}sampling_rate"],
                    timestamps_path=(p := get_timestamps_path(session_or_job, probe_name=probe['name'], band='ap' if band == '_' else 'lfp')),
                    num_samples=np.memmap(p.with_name('continuous.dat'), dtype=np.int16).shape[0] / 384,
                )
            )
    return tuple(results)

def extract_barcodes_from_times(
    on_times: np.ndarray,
    off_times: np.ndarray,
    inter_barcode_interval: float = 29,
    bar_duration: float = 0.015,
    barcode_duration_ceiling: float = 2,
    nbits: int = 32,
    total_time_on_line: float | None = None,
) -> tuple[npt.NDArray[np.float64], npt.NDArray[np.int64]]:
    # from ecephys repo
    """Read barcodes from timestamped rising and falling edges.
    Parameters
    ----------
    on_times : numpy.ndarray
        Timestamps of rising edges on the barcode line
    off_times : numpy.ndarray
        Timestamps of falling edges on the barcode line
    inter_barcode_interval : numeric, optional
        Minimun duration of time between barcodes.
    bar_duration : numeric, optional
        A value slightly shorter than the expected duration of each bar
    barcode_duration_ceiling : numeric, optional
        The maximum duration of a single barcode
    nbits : int, optional
        The bit-depth of each barcode
    total_time_on_line: float, optional
        Timestamp of the last sample on the line (not the last falling edge) used
        to disgard barcodes that are truncated by the end of the recording
    Returns
    -------
    barcode_start_times : list of numeric
        For each detected barcode, the time at which that barcode started
    barcodes : list of int
        For each detected barcode, the value of that barcode as an integer.
    """
    if off_times[0] < on_times[0]:
        off_times = off_times[1:]
    if on_times[-1] > off_times[-1]:
        on_times = on_times[:-1]
    assert len(on_times) == len(
        off_times
    ), f"on and off times should be same length ({len(on_times)} != {len(off_times)})"
    start_indices = np.where(np.diff(on_times) > inter_barcode_interval)[0] + 1
    if on_times[0] > barcode_duration_ceiling:
        # keep first barcode as it occurred sufficiently far from start of recording
        start_indices = np.insert(start_indices, 0, 0)
    # remove times close to end of recording to avoid using truncated barcode
    if total_time_on_line is not None:
        off_times = off_times[
            off_times <= (total_time_on_line - barcode_duration_ceiling)
        ]
        on_times = on_times[on_times < off_times[-1]]
        start_indices = start_indices[start_indices < len(on_times)]
    assert len(on_times) == len(
        off_times
    ), f"on and off times should be same length ({len(on_times)} != {len(off_times)})"
    assert max(start_indices) < len(
        on_times
    ), f"start indices out of bounds ({max(start_indices)} >= {len(on_times)})"

    barcode_start_times = on_times[start_indices]
    barcodes = []
    for _i, t in enumerate(barcode_start_times):
        oncode = on_times[
            np.where(
                np.logical_and(on_times > t, on_times < t + barcode_duration_ceiling)
            )[0]
        ]
        offcode = off_times[
            np.where(
                np.logical_and(off_times > t, off_times < t + barcode_duration_ceiling)
            )[0]
        ]

        currTime = offcode[0]

        bits = np.zeros((nbits,))

        for bit in range(0, nbits):
            nextOn = np.where(oncode > currTime)[0]
            nextOff = np.where(offcode > currTime)[0]

            if nextOn.size > 0:
                nextOn = oncode[nextOn[0]]
            else:
                nextOn = t + inter_barcode_interval

            if nextOff.size > 0:
                nextOff = offcode[nextOff[0]]
            else:
                nextOff = t + inter_barcode_interval

            if nextOn < nextOff:
                bits[bit] = 1

            currTime += bar_duration

        barcode = 0

        # least sig left
        for bit in range(0, nbits):
            barcode += bits[bit] * pow(2, bit)

        barcodes.append(barcode)

    return barcode_start_times, np.array(barcodes, dtype=np.int64)


def find_matching_index(
    master_barcodes: np.ndarray,
    probe_barcodes: np.ndarray,
    alignment_type: Literal["start", "end"] = "start",
) -> tuple[int, int] | tuple[None, None]:
    """Given a set of barcodes for the master clock and the probe clock, find the
    indices of a matching set, either starting from the beginning or the end
    of the list.
    Parameters
    ----------
    master_barcodes : np.ndarray
        barcode values on the master line. One per barcode
    probe_barcodes : np.ndarray
        barcode values on the probe line. One per barcode
    alignment_type : string
        'start' or 'end'
    Returns
    -------
    master_barcode_index : int
        matching index for master barcodes (None if not found)
    probe_barcode_index : int
        matching index for probe barcodes (None if not found)


    >>> find_matching_index(np.array([1, 2, 3]), np.array([1, 2, 3]), alignment_type="start")
    (0, 0)
    >>> find_matching_index(np.array([1, 2, 3]), np.array([1, 2, 3]), alignment_type="end")
    (2, -1)
    >>> find_matching_index(np.array([1, 2, 3]), np.array([4, 5, 6]), alignment_type="start")
    (None, None)

    # in cases of multiple matches (affecting 626791_2022-08-16)
    >>> find_matching_index(np.array([1, 1, 2]), np.array([1, 1, 2]), alignment_type="start")
    (0, 0)
    >>> find_matching_index(np.array([1, 1, 2]), np.array([1, 2]), alignment_type="start")
    (1, 0)
    >>> find_matching_index(np.array([1, 2, 2]), np.array([1, 2, 2]), alignment_type="end")
    (2, -1)
    >>> find_matching_index(np.array([1, 2, 2]), np.array([1, 2]), alignment_type="end")
    (1, -1)
    """
    if alignment_type not in ["start", "end"]:
        raise ValueError(
            "alignment_type must be 'start' or 'end', not " + str(alignment_type)
        )
    foundMatch = False
    master_barcode_index = None

    if alignment_type == "start":
        probe_barcode_index = 0
        direction = 1
    else:
        probe_barcode_index = -1
        direction = -1

    while not foundMatch and abs(probe_barcode_index) < len(probe_barcodes):
        master_barcode_index = np.where(
            master_barcodes == probe_barcodes[probe_barcode_index]
        )[0]
        if (
            len(master_barcode_index) > 1
        ):  # multiple matches only happened once, in 626791_2022-08-16
            # check whether two instances are also found on the probe:
            if probe_barcodes[probe_barcode_index] not in (
                0,
                1,
                2,
                2154270975,
            ):  # tests and known barcode that has multiple matches
                logger.warning(
                    f"Multiple barcode matches found on sync clock: {master_barcode_index=}. "
                    "Using most sensible match, but if this happens frequently there's probably a bug."
                )
            duplicates_on_probe = np.where(
                probe_barcodes == probe_barcodes[probe_barcode_index]
            )[0]
            if alignment_type == "start":
                duplicates_on_master = master_barcode_index[-len(duplicates_on_probe) :]
            else:
                duplicates_on_master = master_barcode_index[: len(duplicates_on_probe)]
            master_barcode_index = np.array(
                [duplicates_on_master[0 if alignment_type == "start" else -1]]
            )

        if len(master_barcode_index) == 1:
            foundMatch = True
        else:
            probe_barcode_index += direction

    if foundMatch and master_barcode_index is not None:
        return master_barcode_index[0], probe_barcode_index
    else:
        return None, None


def match_barcodes(
    master_times: np.ndarray,
    master_barcodes: np.ndarray,
    probe_times: np.ndarray,
    probe_barcodes: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    """Given sequences of barcode values and (local) times on a probe line and a master
    line, find the time points on each clock corresponding to the first and last shared
    barcode.
    If there's only one probe barcode, only the first matching timepoint is returned.
    Parameters
    ----------
    master_times : np.ndarray
        start times of barcodes (according to the master clock) on the master line.
        One per barcode.
    master_barcodes : np.ndarray
        barcode values on the master line. One per barcode
    probe_times : np.ndarray
        start times (according to the probe clock) of barcodes on the probe line.
        One per barcode
    probe_barcodes : np.ndarray
        barcode values on the probe_line. One per barcode
    Returns
    -------
    probe_interval : np.ndarray
        Start and end times of the matched interval according to the probe_clock.
    master_interval : np.ndarray
        Start and end times of the matched interval according to the master clock
    """

    master_start_index, probe_start_index = find_matching_index(
        master_barcodes, probe_barcodes, alignment_type="start"
    )

    if master_start_index is not None:
        t_m_start = master_times[master_start_index]
        t_p_start = probe_times[probe_start_index]
    else:
        t_m_start, t_p_start = None, None

    # print(master_barcodes)
    # print(probe_barcodes)

    # print("Master start index: " + str(master_start_index))
    if len(probe_barcodes) > 1:
        master_end_index, probe_end_index = find_matching_index(
            master_barcodes, probe_barcodes, alignment_type="end"
        )

        if probe_end_index is not None:
            # print("Probe end index: " + str(probe_end_index))
            t_m_end = master_times[master_end_index]
            t_p_end = probe_times[probe_end_index]
        else:
            t_m_end = None
            t_p_end = None
    else:
        t_m_end, t_p_end = None, None

    if not any([t_m_start, t_m_end, t_p_start, t_p_end]):
        raise ValueError(
            "No matching barcodes found between master and probe lines. "
            "Either the sync file did not capture this ephys recording (session mix-up), "
            "or there was a problem with barcode generation or recording (a line disconnected)"
        )
    return np.array([t_p_start, t_p_end]), np.array([t_m_start, t_m_end])


def linear_transform_from_intervals(
    master: np.ndarray | Sequence[float], probe: np.ndarray | Sequence[float]
) -> tuple[float, float]:
    # from ecephys repo
    """Find a scale and translation which aligns two 1d segments
    Parameters
    ----------
    master : iterable
        Pair of floats defining the master interval. Order is [start, end].
    probe : iterable
        Pair of floats defining the probe interval. Order is [start, end].
    Returns
    -------
    scale : float
        Scale factor. If > 1.0, the probe clock is running fast compared to the
        master clock. If < 1.0, the probe clock is running slow.
    translation : float
        If > 0, the probe clock started before the master clock. If > 0, after.
    Notes
    -----
    solves
        (master + translation) * scale = probe
    for scale and translation
    """

    scale = (probe[1] - probe[0]) / (master[1] - master[0])
    translation = probe[0] / scale - master[0]

    return float(scale), float(translation)


def get_probe_time_offset(
    master_times: np.ndarray,
    master_barcodes: np.ndarray,
    probe_times: np.ndarray,
    probe_barcodes: np.ndarray,
    acq_start_index: int,
    local_probe_rate: float,
) -> tuple[float, float, np.ndarray]:
    # from ecephys repo
    """Time offset between master clock and recording probes. For converting probe time to master clock.

    Parameters
    ----------
    master_times : np.ndarray
        start times of barcodes (according to the master clock) on the master line.
        One per barcode.
    master_barcodes : np.ndarray
        barcode values on the master line. One per barcode
    probe_times : np.ndarray
        start times (according to the probe clock) of barcodes on the probe line.
        One per barcode
    probe_barcodes : np.ndarray
        barcode values on the probe_line. One per barcode
    acq_start_index : int
        sample index of probe acquisition start time
    local_probe_rate : float
        the probe's apparent sampling rate

    Returns
    -------
    total_time_shift : float
        Time at which the probe started acquisition, assessed on
        the master clock. If < 0, the probe started earlier than the master line.
    probe_rate : float
        The probe's sampling rate, assessed on the master clock
    master_endpoints : iterable
        Defines the start and end times of the sync interval on the master clock

    """

    probe_endpoints, master_endpoints = match_barcodes(
        master_times, master_barcodes, probe_times, probe_barcodes
    )
    if any(x is None for x in (*probe_endpoints, *master_endpoints)):
        raise ValueError(
            f"Matching barcodes not found: {probe_endpoints=}, {master_endpoints=}"
        )
    rate_scale, time_offset = linear_transform_from_intervals(
        master_endpoints, probe_endpoints
    )

    probe_rate = local_probe_rate * rate_scale
    acq_start_time = acq_start_index / probe_rate

    total_time_shift = time_offset - acq_start_time

    return total_time_shift, probe_rate, master_endpoints


if __name__ == '__main__':
    import doctest
    doctest.testmod()
    ensure_credentials()
    assert_s3_read_access()
    assert_s3_write_access()
    main()