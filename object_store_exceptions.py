"""
2021.11.24 Yixuan Mei
This file contains exceptions in shared memory object store.
"""


class SMOSPortBusy(Exception):
    """
    This exception is raised when there are not enough free ports available
    """
    pass


class SMOSInputFormatError(Exception):
    pass
