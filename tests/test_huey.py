import pathlib
import tempfile
from typing import Generator

import pytest

import np_queuey


@pytest.fixture(scope='module')
def huey_queue() -> Generator[np_queuey.HueyDispatcher, None, None]:
    tempdir = tempfile.mkdtemp()
    huey = np_queuey.HueyDispatcher(f'{tempdir}/huey.db')
    yield huey
        
def test_huey_queue(huey_queue):
    assert pathlib.Path(huey_queue.db_path).exists()

    
    result = huey_queue.submit('add', 1, 2)
    assert result.get() is None
    huey_queue.process()
    assert result.get() == 3
    
if __name__ == '__main__':
    pytest.main()