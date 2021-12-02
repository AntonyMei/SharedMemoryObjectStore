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

    # SharedMemoryObject management
    def create_object(self, name, max_capacity, track_count, block_size_list, track_name_list=None):
        """
        Create a new SharedMemoryObject with given parameters in SMOS.

        :param name: name of the object
        :param max_capacity: maximum number of objects that can be stored in the new SharedMemoryObject
        :param track_count: number of tracks in the new SharedMemoryObject
        :param block_size_list: block size of each track
        :param track_name_list: (optional) name of each track
        :return: always SMOS_SUCCESS
        """
        # create object
        status = safe_execute(target=self.store.create, args=(name, max_capacity, track_count,
                                                              block_size_list, track_name_list, ))
        return status

    def remove_object(self, name):
        """
        Remove SharedMemoryObject specified by name from SMOS. Note that this is potentially
        destructive since all pending accesses to the object will raise FileNotFound error since
        shared memory space is freed.

        :param name: name of object to be removed
        :return: always SMOSSuccess
        """
        # create object
        status = safe_execute(target=self.store.remove, args=(name, ))
        return status

    def put(self, obj, name):
        """TODO: finish this"""
        pass

    def get(self, name):
        """TODO: finish this"""
        pass

    # fine-grained operations for numpy arrays (zero copy)
    # create entry
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
