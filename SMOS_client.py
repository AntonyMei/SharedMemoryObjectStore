"""
2021.12.01 Yixuan Mei
This file contains class Client, which should be instantiated in every process that uses SMOS.
"""

from multiprocessing import shared_memory

import numpy as np

import SMOS_exceptions
import SMOS_server
import SMOS_utils as utils
from SMOS_constants import SMOS_SUCCESS, SMOS_FAIL
from SMOS_utils import safe_execute


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
    def create_object(self, name, max_capacity, track_count, block_size, track_name=None):
        """
        Create a new SharedMemoryObject with given parameters in SMOS.

        :param name: name of the object
        :param max_capacity: maximum number of objects that can be stored in the new SharedMemoryObject
        :param track_count: number of tracks in the new SharedMemoryObject
        :param block_size: block size of each track, a list if there are multiple tracks
        :param track_name: (optional) name of each track, a list if there are multiple tracks
        :return: always SMOS_SUCCESS
        """
        # ensure that block_size and track_name are lists
        if not type(block_size) == list:
            block_size = [block_size]
        if track_name is not None and not type(track_name) == list:
            track_name = [track_name]

        # create object
        status = safe_execute(target=self.store.create, args=(name, max_capacity, track_count,
                                                              block_size, track_name,))
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
        status = safe_execute(target=self.store.remove, args=(name,))
        return status

    def put(self, name, data, as_list=False, redundancy=0):
        """
        Create a SharedMemoryObject with name,  and put data into this SharedMemoryObject.

        :exception SMOS_exceptions.SMOSDimensionMismatch: if multiple entries have different
                   number of tracks

        :param name: name of share memory object
        :param data: data to be put into SMOS
        :param as_list: If as_list = True and data is a list, then each element of the list
               will be stored in a single entry in the SharedMemoryObject. In other cases,
               data will be stored as a whole in one entry and the SharedMemoryObject will
               have only one entry.
        :param redundancy: Require redundancy number of extra free entries in new
               SharedMemoryObject. These extra entries can be used by entry operations.
        :return: always SMOS_SUCCESS
        """
        # format data and check integrity
        if not as_list or not type(data) == list:
            data = [data]
        track_count_list = []
        for entry_idx in range(len(data)):
            if not type(data[entry_idx]) == list:
                data[entry_idx] = [data[entry_idx]]
            track_count_list.append(len(data[entry_idx]))
        if not len(set(track_count_list)) == 1:
            raise SMOS_exceptions.SMOSDimensionMismatch("Multiple entries have different number"
                                                        "of tracks.")
        entry_count = len(data)
        track_count = track_count_list[0]

        # serialize data and calculate size
        serialized_data, data_size_list, data_is_numpy_list = [], [], []
        for data_entry in data:
            serialized_entry = []
            entry_size_list = []
            is_numpy_list = []
            for element in data_entry:
                if type(element) == np.ndarray:
                    serialized_entry.append(element)
                    entry_size_list.append(element.nbytes)
                    is_numpy_list.append(True)
                else:
                    serialized_stream = utils.serialize(element)
                    serialized_entry.append(serialized_stream)
                    entry_size_list.append(len(serialized_stream))
                    is_numpy_list.append(False)
            serialized_data.append(serialized_entry)
            data_size_list.append(entry_size_list)
            data_is_numpy_list.append(is_numpy_list)

        # calculate max size and create SharedMemoryObject
        block_size_list = [max(np.array(data_size_list)[:, i]) for i in range(track_count)]
        self.create_object(name=name, max_capacity=entry_count + redundancy, track_count=track_count,
                           block_size=block_size_list)

        # create entry, write into shared memory and commit
        for entry_idx in range(entry_count):
            # create entry
            serialized_entry = serialized_data[entry_idx]
            is_numpy_list = data_is_numpy_list[entry_idx]
            dtype_list, shape_list = [], []
            for track_idx in range(track_count):
                if is_numpy_list[track_idx]:
                    dtype_list.append(serialized_entry[track_idx].dtype)
                    shape_list.append(serialized_entry[track_idx].shape)
                else:
                    dtype_list.append(None)
                    shape_list.append(None)
            _, object_handle = self.create_entry(name=name, dtype=dtype_list, shape=shape_list,
                                                 is_numpy=is_numpy_list)

            # write into shared memory
            _, buffer_list = self.open_shm(object_handle=object_handle)
            for track_idx in range(track_count):
                if is_numpy_list[track_idx]:
                    buffer_list[track_idx][:] = serialized_entry[track_idx][:]
                else:
                    stream_length = len(serialized_entry[track_idx])
                    buffer_list[track_idx][0:stream_length] = serialized_entry[track_idx]

            # commit
            self.commit_entry(object_handle=object_handle)

        # return
        return SMOS_SUCCESS

    def get(self, name, entry_idx_list=None):
        """
        Get a SharedMemoryObject from shared memory. The object will be reconstructed
        to what it was before being passed into SMOS. Note that this function always
        copies the data from shared memory.

        :param name: name of object to get
        :param entry_idx_list: if entry_idx_list is None, all entries in this
               SharedMemoryObject will be returned
        :return: always [SMOS_SUCCESS, reconstructed_object]
        """
        # determine query range
        if entry_idx_list is None:
            entry_idx_list = self.store.get_entry_idx_list(name=name)

        # read and reconstruct data from shared memory
        return_list = []
        for entry_idx in entry_idx_list:
            # read reconstructed object
            status, handle, obj = self.read_from_object(name=name, entry_idx=entry_idx)
            if status == SMOS_FAIL:
                continue

            # further reconstruction
            if type(obj) == np.ndarray:
                obj = obj.copy()
            if type(obj) == list:
                for track_idx in range(len(obj)):
                    if type(obj[track_idx]) == np.ndarray:
                        obj[track_idx] = obj[track_idx].copy()

            # append to return list and release handle
            return_list.append(obj)
            self.release_entry(object_handle=handle)

        # return
        if len(return_list) == 1:
            return SMOS_SUCCESS, return_list[0]
        else:
            return SMOS_SUCCESS, return_list

    # entry operations
    # fine-grained operations (zero copy)
    #    create procedure:  create_entry  ->  open_shm  ->  commit_entry
    #    r/w procedure:     open_entry    ->  open_shm  ->  release_entry
    #    delete procedure:  delete_entry
    def create_entry(self, name, dtype, shape, is_numpy):
        """
        Create a new entry in given SharedMemoryObject.

        :exception SMOS_exceptions.SMOSDimensionMismatch: if length of input is
                   different of number of tracks

        :param name: name of target SharedMemoryObject
        :param dtype: dtype for each track (None if not numpy), a list if there are multiple tracks
        :param shape: shape for each track (None if not numpy), a list if there are multiple tracks
        :param is_numpy: if track stores numpy array, a list if there are multiple tracks
        :return: [SMOS_SUCCESS, ObjectHandle] if successful
                 [SMOS_FAIL, None] if no free space available in target object
        """
        # ensure that inputs are lists
        if not type(dtype) == list:
            dtype_list = list(dtype)
        else:
            dtype_list = dtype
        if not type(shape) == list:
            shape_list = list(shape)
        else:
            shape_list = shape
        if not type(is_numpy) == list:
            is_numpy_list = list(is_numpy)
        else:
            is_numpy_list = is_numpy

        # check input integrity
        track_count = safe_execute(target=self.store.get_track_count, args=(name,))
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
                                                 args=(name, entry_config_list,))

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
                                                 args=(name, entry_idx,))

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
                                      args=(object_handle.name, object_handle.entry_config_list,))
        _, block_size_list = safe_execute(target=self.store.get_block_size_list,
                                          args=(object_handle.name,))
        _, shm_name_list = safe_execute(target=self.store.get_shm_name_list,
                                        args=(object_handle.name,))

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
                                    args=(object_handle.name, object_handle.entry_config_list,))

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
                              args=(object_handle.name, object_handle.entry_idx,))

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
                              args=(name, entry_idx, force_delete,))
        return status

    # coarse-grained operations (queue API)
    #   write process: push_to_object
    #   read process:  pop_from_object  ->  free_handle
    #                  read_from_object ->  release_entry
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
                                                 args=(name, force_pop,))

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
                deserialized_object = utils.deserialize(data_stream=buffer)
                buffer.release()
                reconstructed_object.append(deserialized_object)

        # return
        if len(reconstructed_object) == 1:
            return SMOS_SUCCESS, object_handle, reconstructed_object[0]
        else:
            return SMOS_SUCCESS, object_handle, reconstructed_object

    def read_from_object(self, name, entry_idx):
        """
        Read an entry from target SharedMemoryObject. This function reconstructs data
        to what it was before being passed into SMOS.

        :param name: name of the SharedMemoryObject
        :param entry_idx: index of entry to be read
        :return: [SMOS_SUCCESS, object_handle, reconstructed_object] if successful
                 [SMOS_FAIL, None, None] if entry does not exist in target SharedMemoryObject
        """
        # pop entry config
        status, entry_config_list = safe_execute(target=self.store.read_entry_config,
                                                 args=(name, entry_idx,))

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
        object_handle.entry_idx = entry_idx
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
                deserialized_object = utils.deserialize(data_stream=buffer)
                buffer.release()
                reconstructed_object.append(deserialized_object)

        # return
        if len(reconstructed_object) == 1:
            return SMOS_SUCCESS, object_handle, reconstructed_object[0]
        else:
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

    def push_to_object(self, name, data):
        """
        Push data to target SharedMemoryObject. Note that if data is a list, then each element
        in the list will be assigned to one track in SharedMemoryObject.

        :exception SMOS_exceptions.SMOSDimensionMismatch: if the number of elements in data is
                   different from track_count of target SharedMemoryObject

        :param name: name of the SharedMemoryObject
        :param data: data to be pushed
        :return: [SMOS_SUCCESS, entry_idx] if successful
                 [SMOS_FAIL, None] if no free space available in target object
        """
        # ensure that data is a list
        if not type(data) == list:
            data = [data]

        # check input dimension
        track_count = safe_execute(target=self.store.get_track_count, args=(name,))
        if not len(data) == track_count:
            raise SMOS_exceptions.SMOSDimensionMismatch(f"There are {track_count} tracks, but input data"
                                                        f"has length {len(data)}.")

        # compute input data configuration
        dtype_list, shape_list, is_numpy_list = [], [], []
        for data_element in data:
            if type(data_element) == np.ndarray:
                dtype_list.append(data_element.dtype)
                shape_list.append(data_element.shape)
                is_numpy_list.append(True)
            else:
                dtype_list.append(None)
                shape_list.append(None)
                is_numpy_list.append(False)

        # create new entry
        status, object_handle = self.create_entry(name=name, dtype=dtype_list, shape=shape_list,
                                                  is_numpy=is_numpy_list)
        if status == SMOS_FAIL:
            return SMOS_FAIL, None

        # serialize input data
        serialized_data_list = []
        for data_element in data:
            if type(data_element) == np.ndarray:
                serialized_data_list.append(data_element)
            else:
                stream = utils.serialize(data_element)
                serialized_data_list.append(stream)

        # copy into shared memory
        _, buffer_list = self.open_shm(object_handle=object_handle)
        for track_idx in track_count:
            if is_numpy_list[track_idx]:
                buffer_list[track_idx][:] = serialized_data_list[track_idx][:]
            else:
                stream_length = len(serialized_data_list[track_idx])
                buffer_list[track_idx][0:stream_length] = serialized_data_list[track_idx]

        # commit entry and return
        _, entry_idx = self.commit_entry(object_handle=object_handle)
        return SMOS_SUCCESS, entry_idx
