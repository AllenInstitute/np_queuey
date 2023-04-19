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
from np_queuey.dispatchers.bases import HueyDispatcher, JobDispatcher

logger = np_logging.getLogger(__name__)


class JobProcessor(abc.ABC):
    
    @abc.abstractmethod
    def process(
        self,
        dispatcher: Optional[JobDispatcher] = None,
        *args,
        **kwargs,
        ) -> None:
        """Process jobs.
        
        Processing order determined by concretion.
        """
        
class HueyProcessor(JobProcessor):
    """Minimal class for processing huey jobs.
    
    Almost all configuration is done in the dispatcher.
    """
    
    def __init__(
        self,
        dispatcher: Optional[HueyDispatcher] = None,
        ) -> None:
        self.dispatcher = dispatcher
        self.huey = dispatcher.huey_class
        