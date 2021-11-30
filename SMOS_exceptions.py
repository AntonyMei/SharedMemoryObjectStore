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


class SMOSReadRefDoubleRelease(Exception):
    """
    This exception is raised when read reference to an entry is released multiple
    times. Note that this exception may not be raised exactly when double release
    happens, but a double release is sure to cause this exception in itself or
    later releases.
    """
    pass


class SMOSBlockDoubleRelease(Exception):
    """
    This exception is raised when the block to be freed is already in free block
    list when deleting an entry form a data track. This is probably caused by
    erroneously mapping two entries to the same block or calling free_block_mapping
    twice.
    """
    pass


class SMOSTrackMismatch(Exception):
    """
    This exception is raised when track_name of current track is different from input
    entry_config's track_name.
    """
    pass


class SMOSDimensionMismatch(Exception):
    """
    This exception is raised when multiple items that are supposed to have the same
    shape turn out to be different.
    """
    pass


class SMOSTrackUnaligned(Exception):
    """
    This exception is raised when multiple tracks in the same SharedMemoryObject are
    unaligned, i.e. they have different remaining block count.
    """
    pass
