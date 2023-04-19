from __future__ import annotations

import abc
import datetime
import imp
import importlib
import pathlib
from typing import Any, Callable, ClassVar, Optional, Type

import huey
import huey.api
import huey.consumer
import huey.consumer_options
import np_config
import np_logging

import np_queuey.tasks as tasks
import np_queuey.utils as utils
from np_queuey.dispatchers.bases import HueyDispatcher

logger = np_logging.getLogger(__name__)

   
class SqliteHueyDispatcher(HueyDispatcher):
    """Job queue using `SqliteHuey` on varying hosts."""
    
    huey_type: ClassVar[Type[huey.Huey]] = huey.SqliteHuey
    """The `huey` class, determining storage used."""

            
    @classmethod
    def get_huey_config(cls, host: str) -> dict[str, str | int | bool]:
        return dict(
            name=cls.get_huey_name(host),
            filename=cls.get_sqlite_db(host).as_posix(),
            journal_mode='truncate',    # 'wal' not supported on NAS
            fsync=True,                 # suggested if 'wal' disabled
        )
    
    @classmethod
    def get_sqlite_db(cls, host: str) -> pathlib.Path:
        """Path to the sqlite database for a host's `huey`"""
        return pathlib.Path(utils.DEFAULT_HUEY_DIR) / f'{cls.get_huey_name(host)}.db'
    
        
class ParallelSqliteHueyDispatcher(SqliteHueyDispatcher):
    """Parallel queue."""
    
    @classmethod
    def get_huey_name(cls, host: str) -> str:
        """
        >>> __class__.get_name('localhost')
        'LOCALHOST_parallel'
        """
        return f'{host.upper()}_parallel'
    
    @classmethod
    def get_consumer_options(cls, host: str) -> tuple[str, ...]:
        options: dict[str, list[str]] = utils.CONFIG['consumer_options']
        default_parallel = options['parallel']
        return super().get_consumer_options(host) + tuple(default_parallel)

    
class SerialSqliteHueyDispatcher(SqliteHueyDispatcher):
    """FIFO queue."""
    @classmethod
    def get_huey_config(cls, host: str) -> dict[str, str | int | bool]:
        default = super().get_huey_config(host)
        default.pop('strict_fifo', None)
        return dict(
            strict_fifo=True,
            **default,
        )
        
