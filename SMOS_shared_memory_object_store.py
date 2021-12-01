"""
2021.12.1
This file contains class SharedMemoryObjectStore, which is core of SMOS.
"""

import SMOS_exceptions
import SMOS_utils as utils
from SMOS_shared_memory_object import SharedMemoryObject
from SMOS_constants import SMOS_FAIL, SMOS_SUCCESS, SMOS_PERMISSION_DENIED


class SharedMemoryObjectStore:

    def __init__(self):
        """
        SharedMemoryObjectStore is core to SMOS. It manages all SharedMemoryObjects
        and respond to queries. This class should be initialized using SMOSServer and
        accessed from a SMOSClient.
        """
        self.object_dict = {}
        self.global_lock = utils.RWLock()

    # object manipulation
    def create(self, name, max_capacity, track_count, block_size_list, track_name_list=None):
        """
        Create a new SharedMemoryObject with given parameters in SMOS.

        :exception SMOS_exceptions.SMOSObjectExistError: if object with same name already exists

        :param name: name of the object
        :param max_capacity: maximum number of objects that can be stored in the new SharedMemoryObject
        :param track_count: number of tracks in the new SharedMemoryObject
        :param block_size_list: block size of each track
        :param track_name_list: (optional) name of each track
        :return: always SMOS_SUCCESS
        """
        # check if object already exists
        if name in self.object_dict:
            raise SMOS_exceptions.SMOSObjectExistError(f"Object with name {name} already exists.")

        # create SharedMemoryObject
        self.global_lock.writer_enter()
        self.object_dict[name] = SharedMemoryObject(name=name, max_capacity=max_capacity, track_count=track_count,
                                                    block_size_list=block_size_list, track_name_list=track_name_list)
        self.global_lock.writer_leave()

        # return
        return SMOS_SUCCESS

    def remove(self, name):
        """
        Remove SharedMemoryObject specified by name from SMOS. Note that this is potentially
        destructive since all pending accesses to the object will raise FileNotFound error since
        shared memory space is freed.

        :exception SMOS_exceptions.SMOSObjectNotFoundError: if object to be deleted does not exist

        :param name: name of object to be removed
        :return: always SMOSSuccess
        """
        # check if object exists
        if name not in self.object_dict:
            raise SMOS_exceptions.SMOSObjectNotFoundError(f"Object with name {name} not found.")

        # delete SharedMemoryObject
        self.global_lock.writer_enter()
        shm_object = self.object_dict[name]
        shm_object.stop()
        del shm_object
        del self.object_dict[name]
        self.global_lock.writer_leave()

        # return
        return SMOS_SUCCESS
