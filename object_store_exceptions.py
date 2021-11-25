"""
2021.11.24 Yixuan Mei
This file contains exceptions in shared memory object store.
"""


class SMOSPortBusy(Exception):
    """
    This exception is raised when there are not enough free ports available.
    """
    pass


class SMOSInputTypeError(Exception):
    """
    This exception is raised when input type is not the same as expected.
    """
    pass


class SMOSEntryUnallocated(Exception):
    """
    This exception is raised when user tries to append an unallocated block
    into data tracks.
    """
    pass
