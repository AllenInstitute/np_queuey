from __future__ import annotations

import abc
import contextlib
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

logger = np_logging.getLogger(__name__)


class JobDispatcher(abc.ABC):
    
    @abc.abstractmethod
    def submit(
        self,
        host: str,
        job: str | Callable,
        *args,
        **kwargs,
    ) -> None:
        """Submit `job` to the queue on `host` in open-loop.
        
        - *args and **kwargs are passed to `job`
        
        Processing order determined by concretion.
        """


class HueyDispatcher(JobDispatcher):
    """ABC for a job dispatcher using `huey` on varying hosts."""
    
    huey_type: ClassVar[Type[huey.Huey]]
    """The `huey` class, determining storage used."""
    
    host_to_huey: ClassVar[dict[str, huey.Huey]] = {}
    """Map host to `huey` instance, for storage."""
    
    @classmethod
    def get_huey(cls, host: str) -> huey.Huey:
        """Get the `huey` instance for a host."""
        with contextlib.suppress(KeyError):
            return cls.host_to_huey[host]
        _huey = cls.huey_type(
            **cls.get_huey_config(host)
        )
        cls.host_to_huey[host] = _huey
        return cls.get_huey(host)
    
    @classmethod
    @abc.abstractmethod
    def get_huey_config(cls, host: str) -> dict[str, Any]:
        """Get the config for the `huey` instance on a host."""
    
    @classmethod
    def get_huey_tasks(cls, host: str, tasks_module: Optional[pathlib.Path] = None) -> dict[str, Callable]:
        """Get the jobs ("tasks" in huey) available for dispatch to a host."""
        tasks_module = tasks_module or pathlib.Path(utils.DEFAULT_HUEY_DIR) / f'{cls.get_huey_name(host)}.py'
        tasks = imp.load_source(tasks_module.stem, tasks_module.as_posix())
        return {task: getattr(tasks, task) for task in dir(tasks) if isinstance(getattr(tasks, task), Callable)}
    
    @classmethod
    def get_scheduled_tasks(cls, host: str) -> dict[str, Callable]:
        """Get the scheduled jobs ("tasks" in huey) for a host."""
        return cls.get_huey_tasks(host, pathlib.Path(utils.DEFAULT_HUEY_DIR) / f'{cls.get_huey_name(host)}_scheduled.py')
    
    @classmethod
    def get_huey_name(cls, host: str) -> str:
        """Get a unique name for the dispatcher's `huey` instance on a host, 
        e.g. `f'{host.upper()}_parallel`, `f'{host.upper()}_serial'`, ...
        
        Used for config, paths, etc.
        """
        return host.upper()

    @classmethod
    def get_consumer_options(cls, host: str) -> tuple[str, ...]:
        """Get the args to pass to the `huey_consumer` command.
        
        See https://huey.readthedocs.io/en/latest/consumer.html#options-for-the-consumer
        """
        options: dict[str, list[str]] = utils.CONFIG['consumer_options']
        default = options['default']
        hosts = options.get(cls.get_huey_name(host), [])
        return tuple(default or hosts)
        
    def submit(
        self,
        host: str,
        job: str | Callable,
        huey_task_args: Optional[dict[str, Any]] = None,
        huey_schedule_args: Optional[dict[str, Any]] = None,
        *args,
        **kwargs
    ) -> None:
        """Submit `job` to the queue on `host` in open-loop, with any
        required args.
        """
        huey = self.get_huey(host)
        task = job if isinstance(job, Callable) else self.get_huey_tasks(host)[job]
        assert task in self.__dict__.values(), f'{task} not in {self.__dict__}'
        task_args = huey_task_args or {}
        schedule_args = huey_schedule_args or {}
        self.get_huey(host).task(**task_args)(task).schedule(tuple(*args, **kwargs), **schedule_args)

    def process(
        self,
        *args,
        **kwargs,
    ) -> None:
        
        huey = self.get_huey(np_config.HOSTNAME)
        
        
        subprocess.Popen(
            'huey_consumer np_queuey.hueys.dynamicrouting_behavior_session_mtrain_upload.huey -l logs/huey.log',
        creationflags=subprocess.CREATE_NEW_CONSOLE,
        )
        