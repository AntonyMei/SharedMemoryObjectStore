"""
2021.12.01 Yixuan Mei
This file contains class Client, which should be instantiated in every process that uses SMOS.
"""

import multiprocessing as mp
import time
from multiprocessing.managers import BaseManager

import SMOS_server
import SMOS_exceptions
import SMOS_utils as utils
from SMOS_utils import safe_execute
from SMOS_constants import SMOS_SUCCESS, SMOS_FAIL
from SMOS_shared_memory_object_store import SharedMemoryObjectStore


class Client:

    def __init__(self, connection: utils.ConnectionDescriptor):
        """
        Each Client instance connects to one existing server to access SMOS
        object storage services.

        :param connection: a ConnectionDescriptor that specifies address of SMOS server
        """
        self.connection = connection
        self.store = SMOS_server.get_object_store(connection=connection)

    # fine-grained operations for numpy arrays (zero copy)
    # write
    def create_entry(self, name, dtype_list, shape_list):
        """
        Create a new entry in given SharedMemoryObject.

        :exception SMOS_exceptions.SMOSDimensionMismatch: if length of input is
                   different of number of tracks

        :param name: name of target SharedMemoryObject
        :param dtype_list: dtype for each track
        :param shape_list: shape for each track
        :return: [SMOS_SUCCESS, ObjectHandle] if successful
                 [SMOS_FAIL, None] if no free space available in target object
        """
        # check input integrity
        track_count = safe_execute(target=self.store.get_track_count, args=(name, ))
        if not len(dtype_list) == len(shape_list) == track_count:
            raise SMOS_exceptions.SMOSDimensionMismatch(f"There are {track_count} tracks, but only "
                                                        f"{len(dtype_list)} input dtypes and {len(shape_list)}"
                                                        f"input shapes.")

        # prepare entry config
        entry_config_list = []
        for track_idx in range(track_count):
            entry_config = utils.EntryConfig(dtype=dtype_list[track_idx], shape=shape_list[track_idx],
                                             is_numpy=True)
            entry_config_list.append(entry_config)

        # allocate block
        status, entry_config_list = safe_execute(target=self.store.allocate_block,
                                                 args=(name, entry_config_list, ))

        # build object handle and return
        if status == SMOS_SUCCESS:
            object_handle = utils.ObjectHandle()
            object_handle.name = name
            object_handle.track_count = track_count
            object_handle.entry_config_list = entry_config_list
            return SMOS_SUCCESS, object_handle
        else:
            return SMOS_FAIL, None
