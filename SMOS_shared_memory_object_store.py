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
        # check if name already exists
        if name in self.object_dict:
            raise SMOS_exceptions.SMOSObjectExistError(f"Object with name {name} already exists.")

        # create SharedMemoryObject
        self.global_lock.writer_enter()
        self.object_dict[name] = SharedMemoryObject(name=name, max_capacity=max_capacity, track_count=track_count,
                                                    block_size_list=block_size_list, track_name_list=track_name_list)
        self.global_lock.writer_leave()
        return SMOS_SUCCESS
