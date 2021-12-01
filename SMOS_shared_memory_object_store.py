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

    # entry manipulation
    # write
    def allocate_block(self, name, entry_config_list: [utils.EntryConfig]):
        """
        Allocate block for new entry in SharedMemoryObject with given name.

        :exception SMOS_exceptions.SMOSObjectNotFoundError: if target SharedMemoryObject
                   does not exist

        :param name: name of the SharedMemoryObject
        :param entry_config_list: configurations of new entry, one for each track
        :return: [SMOS_SUCCESS, entry_config_list] if successful,
                 [SMOS_FAIL, None] if no free block available
        """
        # check if object exists
        if name not in self.object_dict:
            raise SMOS_exceptions.SMOSObjectNotFoundError(f"Object with name {name} not found.")

        # allocate block
        self.global_lock.reader_enter()
        status = self.object_dict[name].allocate_block(entry_config_list=entry_config_list)
        self.global_lock.reader_leave()

        # return
        if status == SMOS_SUCCESS:
            return SMOS_SUCCESS, entry_config_list
        else:
            return SMOS_FAIL, None

    def append_entry_config(self, name, entry_config_list: [utils.EntryConfig]):
        """
        Append configurations of a new entry into given SharedMemoryObject.

        :exception SMOS_exceptions.SMOSObjectNotFoundError: if target SharedMemoryObject
                   does not exist

        :param name: name of the SharedMemoryObject
        :param entry_config_list: configurations of new entry, one for each track
        :return: always SMOS_SUCCESS
        """
        # check if object exists
        if name not in self.object_dict:
            raise SMOS_exceptions.SMOSObjectNotFoundError(f"Object with name {name} not found.")

        # append entry config
        self.global_lock.reader_enter()
        self.object_dict[name].append_entry_config(entry_config_list=entry_config_list)
        self.global_lock.reader_leave()

        # return
        return SMOS_SUCCESS

    # read
    def read_entry_config(self, name, idx):
        """
        Read entry configuration at given index form given SharedMemoryObject.

        :exception SMOS_exceptions.SMOSObjectNotFoundError: if target SharedMemoryObject
                   does not exist

        :param name: name of the SharedMemoryObject
        :param idx: index of entry to be read
        :return: [SMOS_SUCCESS, entry_config_list] if successful,
                 [SMOS_FAIL, None] if index out of range
        """
        # check if object exists
        if name not in self.object_dict:
            raise SMOS_exceptions.SMOSObjectNotFoundError(f"Object with name {name} not found.")

        # read
        self.global_lock.reader_enter()
        status, entry_config_list = self.object_dict[name].read_entry_config(idx=idx)
        self.global_lock.reader_leave()

        # return
        return status, entry_config_list

    def release_read_reference(self, name, idx):
        """
        Release read reference on given entry from given SharedMemoryObject.

        :exception SMOS_exceptions.SMOSObjectNotFoundError: if target SharedMemoryObject
                   does not exist

        :param name: name of the SharedMemoryObject
        :param idx: index of entry to be released
        :return: SMOS_SUCCESS if successful,
                 SMOS_FAIL if index out of range
        """
        # check if object exists
        if name not in self.object_dict:
            raise SMOS_exceptions.SMOSObjectNotFoundError(f"Object with name {name} not found.")

        # release read reference
        self.global_lock.reader_enter()
        status = self.object_dict[name].release_read_reference(idx=idx)
        self.global_lock.reader_leave()

        # return
        return status

    # delete
    def delete_entry_config(self, name, idx, force_delete=False):
        """
        Delete entry at given index from given SharedMemoryObject. Note that this is lazy
        delete, the actual data in shared memory is not erased.

        :exception SMOS_exceptions.SMOSObjectNotFoundError: if target SharedMemoryObject
                   does not exist

        :param name: name of the SharedMemoryObject
        :param idx: index of entry to be deleted
        :param force_delete: whether to delete the entry when there are still pending readers
        :return: SMOS_SUCCESS if successful,
                 SMOS_FAIL if index out of range,
                 SMOS_PERMISSION_DENIED if permission denied
        """
        # check if object exists
        if name not in self.object_dict:
            raise SMOS_exceptions.SMOSObjectNotFoundError(f"Object with name {name} not found.")

        # delete entry config
        self.global_lock.reader_enter()
        status = self.object_dict[name].delete_entry_config(idx=idx, force_delete=force_delete)
        self.global_lock.reader_leave()

        # return
        return status

    # pop and free
    def pop_entry_config(self, name, force_pop=False):
        """
        Pop an entry from SharedMemoryObject. Note that blocks to which the entry is mapped
        will not be freed in this function (since data in the entry will be used after pop).
        Call free_block_mapping to free the block when data in the entry is no longer useful.

        :exception SMOS_exceptions.SMOSObjectNotFoundError: if target SharedMemoryObject
                   does not exist

        :param name: name of the SharedMemoryObject
        :param force_pop: whether to pop the entry when there are still pending readers
        :return: [SMOS_SUCCESS, entry_config_list] if successful,
                 [SMOS_FAIL, None] if data track empty,
                 [SMOS_PERMISSION_DENIED, None] if permission denied
        """
        # check if object exists
        if name not in self.object_dict:
            raise SMOS_exceptions.SMOSObjectNotFoundError(f"Object with name {name} not found.")

        # pop entry config
        self.global_lock.reader_enter()
        status, entry_config_list = self.object_dict[name].pop_entry_config(force_pop=force_pop)
        self.global_lock.reader_leave()

        # return
        return status, entry_config_list

    def free_block_mapping(self, name, entry_config_list: [utils.EntryConfig]):
        """
        Free blocks associated with a previously popped entry.

        :exception SMOS_exceptions.SMOSObjectNotFoundError: if target SharedMemoryObject
                   does not exist

        :param name: name of the SharedMemoryObject
        :param entry_config_list: configurations of entry to be freed
        :return: always SMOS_SUCCESS
        """
        # check if object exists
        if name not in self.object_dict:
            raise SMOS_exceptions.SMOSObjectNotFoundError(f"Object with name {name} not found.")

        # free block mapping
        self.global_lock.reader_enter()
        self.object_dict[name].free_block_mapping(entry_config_list=entry_config_list)
        self.global_lock.reader_leave()

        # return
        return SMOS_SUCCESS

    # stop
    def stop(self):
        """
        Stop SharedMemoryObjectStore for safe exit

        :return: always SMOS_Success
        """
        # stop all objects
        for shm_object in list(self.object_dict.values()):
            shm_object.stop()

        # release lock
        del self.global_lock

    # utility functions
    def get_shm_names(self, object_name):
        """
        Get names of all shared memories associated with given SharedMemoryObject.

        :exception SMOS_exceptions.SMOSObjectNotFoundError: if target SharedMemoryObject
                   does not exist

        :param object_name: name of the SharedMemoryObject
        :return: list of shared memory names
        """
        # check if object exists
        if name not in self.object_dict:
            raise SMOS_exceptions.SMOSObjectNotFoundError(f"Object with name {name} not found.")

        # get names
        self.global_lock.reader_enter()
        shm_name_list = self.object_dict[object_name].get_shm_name_list()
        self.global_lock.reader_leave()

        # return
        return shm_name_list

    def get_entry_offset(self, name, entry_config_list: [utils.EntryConfig]):
        """
        Get offset of each track for given entry in shared memory space.

        :exception SMOS_exceptions.SMOSObjectNotFoundError: if target SharedMemoryObject
                   does not exist

        :param name: name of the SharedMemoryObject
        :param entry_config_list: configurations of entry to be queried
        :return: always [SMOS_SUCCESS, offset_list]
        """
        # check if object exists
        if name not in self.object_dict:
            raise SMOS_exceptions.SMOSObjectNotFoundError(f"Object with name {name} not found.")

        # get offset
        self.global_lock.reader_enter()
        status, offset_list = self.object_dict[name].get_entry_offset(entry_config_list=entry_config_list)
        self.global_lock.reader_leave()

        # return
        return status, offset_list

    def profile(self):
        """
        Profiles SMOS usage.
        """
        # fetch data
        self.global_lock.reader_enter()
        name_list = []
        entry_count_list = []
        capacity_list = []
        for shm_object in list(self.object_dict.values()):
            name_list.append(shm_object.name)
            entry_count_list.append(shm_object.get_entry_count())
            capacity_list.append(shm_object.max_capacity)
        self.global_lock.reader_leave()

        # print to terminal
        for idx in range(len(name_list)):
            utils.log2terminal(info_type="Info", msg=f"{name_list[idx]}: {entry_count_list[idx]}"
                                                     f"/{capacity_list[idx]}")

