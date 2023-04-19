import np_config

from np_queuey.dispatchers.bases import (ParallelSqliteHueyDispatcher,
                                         SerialSqliteHueyDispatcher)

serial = SerialSqliteHueyDispatcher.get_huey(np_config.HOSTNAME)
parallel = ParallelSqliteHueyDispatcher.get_huey(np_config.HOSTNAME)