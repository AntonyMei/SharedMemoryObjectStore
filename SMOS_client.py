"""
2021.12.01 Yixuan Mei
This file contains class Client, which should be instantiated in every process that uses SMOS.
"""

import multiprocessing as mp
import time
from multiprocessing.managers import BaseManager
from multiprocessing import shared_memory

import numpy as np

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

    # SharedMemoryObject operations
    #   workflow 1:  create_object  ->  [entry operations]  ->  remove_object
    #   workflow 2:  put            ->  (multiple) get
    # These two usages can be combined.
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

    def put(self, name, obj):
        """TODO: finish this"""
        pass

    def get(self, name):
        """TODO: finish this"""
        pass

    # entry operations
    # fine-grained operations (zero copy)
    #    create procedure:  create_entry  ->  open_shm  ->  commit_entry
    #    r/w procedure:     open_entry    ->  open_shm  ->  release_entry
    #    delete procedure:  delete_entry
    def create_entry(self, name, dtype_list, shape_list, is_numpy_list):
        """
        Create a new entry in given SharedMemoryObject.

        :exception SMOS_exceptions.SMOSDimensionMismatch: if length of input is
                   different of number of tracks

        :param name: name of target SharedMemoryObject
        :param dtype_list: dtype for each track (None if not numpy)
        :param shape_list: shape for each track (None if not numpy)
        :param is_numpy_list: if track stores numpy array
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
                                             is_numpy=is_numpy_list[track_idx])
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

    def open_entry(self, name, entry_idx):
        """
        Open entry specified by entry_idx from target SharedMemoryObject.

        :param name: name of the SharedMemoryObject
        :param entry_idx: index of entry to be read
        :return: [SMOS_SUCCESS, ObjectHandle] if successful
                 [SMOS_FAIL, None] if index out of range
        """
        # get entry config
        status, entry_config_list = safe_execute(target=self.store.read_entry_config,
                                                 args=(name, entry_idx, ))

        # build object handle
        if status == SMOS_SUCCESS:
            object_handle = utils.ObjectHandle()
            object_handle.name = name
            object_handle.entry_idx = entry_idx
            object_handle.track_count = len(entry_config_list)
            object_handle.entry_config_list = entry_config_list
            return SMOS_SUCCESS, object_handle
        else:
            return SMOS_FAIL, None

    def open_shm(self, object_handle: utils.ObjectHandle):
        """
        Open data stored in target entry from shared memory space. For tracks that
        store numpy array, the return value is a shared memory backed numpy array.
        For tracks that store other customized types of data, return value is raw
        buffer (shm.buf).

        :param object_handle: handle of target entry
        :return: always [SMOS_SUCCESS, a list of shm.buf / shm backed numpy array]
        """
        # get offset and shared memory name
        _, offset_list = safe_execute(target=self.store.get_entry_offset_list,
                                      args=(object_handle.name, object_handle.entry_config_list, ))
        _, block_size_list = safe_execute(target=self.store.get_block_size_list,
                                          args=(object_handle.name, ))
        _, shm_name_list = safe_execute(target=self.store.get_shm_name_list,
                                        args=(object_handle.name, ))

        # open shared memory and get data
        return_list = []
        for track_idx in range(object_handle.track_count):
            # open shm
            shm = shared_memory.SharedMemory(name=shm_name_list[track_idx])
            object_handle.shm_list.append(shm)

            # open data in different formats
            entry_config = object_handle.entry_config_list[track_idx]
            if entry_config.is_numpy:
                mm_array = np.ndarray(shape=entry_config.shape, dtype=entry_config.dtype,
                                      buffer=shm.buf, offset=offset_list[track_idx])
                return_list.append(mm_array)
            else:
                buffer = shm.buf[offset_list[track_idx]: offset_list[track_idx] + block_size_list[track_idx]]
                object_handle.buf_list.append(buffer)
                return_list.append(buffer)

        # return
        return SMOS_SUCCESS, return_list

    def commit_entry(self, object_handle: utils.ObjectHandle):
        """
        Commit the entry to SMOS. After this operation the entry will be visible to
        all other processes. Note that after commit, the object_handle will be destroyed.

        :param object_handle: handle of target entry
        :return: always [SMOS_SUCCESS, index of newly committed entry]
        """
        # commit entry config
        _, entry_idx = safe_execute(target=self.store.append_entry_config,
                                    args=(object_handle.name, object_handle.entry_config_list, ))

        # clean up
        for buffer in object_handle.buf_list:
            buffer.release()
        for shm in object_handle.shm_list:
            shm.close()

        # return
        return SMOS_SUCCESS, entry_idx

    def release_entry(self, object_handle: utils.ObjectHandle):
        """
        Release read reference to target entry when reading is finished. Note that
        after this operation, object_handle is destroyed.

        :param object_handle: entry to be released
        :return: SMOS_SUCCESS if successful,
                 SMOS_FAIL if target entry does not exist (has been deleted)
        """
        # release read reference
        status = safe_execute(target=self.store.release_read_reference,
                              args=(object_handle.name, object_handle.entry_idx, ))

        # clean up
        if status == SMOS_SUCCESS:
            for buffer in object_handle.buf_list:
                buffer.release()
            for shm in object_handle.shm_list:
                shm.close()

        # return
        return status

    def delete_entry(self, name, entry_idx, force_delete=False):
        """
        Delete target entry from SharedMemoryObject specified by name. Note that this is lazy
        delete, the actual data in shared memory is not erased.

        :param name: name of the SharedMemoryObject
        :param entry_idx: index of entry to be deleted
        :param force_delete: whether to delete the entry when there are still pending readers
        :return: SMOS_SUCCESS if successful,
                 SMOS_FAIL if target entry does not exist,
                 SMOS_PERMISSION_DENIED if permission denied
        """
        # delete entry
        status = safe_execute(target=self.store.delete_entry_config,
                              args=(name, entry_idx, force_delete, ))
        return status

    # coarse-grained operations (queue API)
    #   write process: push_to_object
    #   read process:  pop_from_object  ->  free_handle
    def pop_from_object(self, name, force_pop=False):
        """
        Pop an entry from target object. This function reconstructs data to what it
        was before being passed into SMOS.

        :param name: name of the SharedMemoryObject
        :param force_pop: whether to pop an entry when there are still pending readers
        :return: [SMOS_SUCCESS, object_handle, reconstructed_object] if successful
                 [SMOS_FAIL, None, None] if target SharedMemoryObject is empty
                 [SMOS_PERMISSION_DENIED, None, None] if permission denied
        """
        # pop entry config
        status, entry_config_list = safe_execute(target=self.store.pop_entry_config,
                                                 args=(name, force_pop, ))

        # check if we successfully get entry config
        if not status == SMOS_SUCCESS:
            return status, None, None

        # get shared memory info
        _, offset_list = safe_execute(target=self.store.get_entry_offset_list,
                                      args=(name, entry_config_list,))
        _, block_size_list = safe_execute(target=self.store.get_block_size_list, args=(name,))
        _, shm_name_list = safe_execute(target=self.store.get_shm_name_list, args=(name,))

        # build object handle (partial build)
        object_handle = utils.ObjectHandle()
        object_handle.name = name
        object_handle.entry_config_list = entry_config_list

        # reconstruct object
        reconstructed_object = []
        for track_idx in range(len(entry_config_list)):
            shm = shared_memory.SharedMemory(name=shm_name_list[track_idx])
            entry_config = entry_config_list[track_idx]
            if entry_config.is_numpy:
                mm_array = np.ndarray(shape=entry_config.shape, dtype=entry_config.dtype,
                                      buffer=shm.buf, offset=offset_list[track_idx])
                object_handle.shm_list.append(shm)
                reconstructed_object.append(mm_array)
            else:
                buffer = shm.buf[offset_list[track_idx]: offset_list[track_idx] + block_size_list[track_idx]]
                deserialized_object = utils.deserialize(buffer)
                buffer.release()
                reconstructed_object.append(deserialized_object)

        # return
        return SMOS_SUCCESS, object_handle, reconstructed_object

    def free_handle(self, object_handle: utils.ObjectHandle):
        """
        Free an object_handle returned by a previous pop_from_object. This operation will
        release all SMOS resources related with the handle. reconstructed_object's shared
        memory arrays can no longer be accessed once this function is called.

        :param object_handle: object handle to free
        :return: always SMOS_SUCCESS
        """
        # free block mapping of underlying entry
        _ = safe_execute(target=self.store.free_block_mapping,
                         args=(object_handle.name, object_handle.entry_config_list))

        # release resources
        for shm in object_handle.shm_list:
            shm.close()

        # return
        return SMOS_SUCCESS

    def push_to_object(self, name, obj):
        pass
