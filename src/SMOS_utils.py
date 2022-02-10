"""
2021.11.24 Yixuan Mei
This file contains utility functions for Shared Memory Object Store
"""

import multiprocessing
import os
import pickle
import socket
from multiprocessing import resource_tracker

import numpy as np

import SMOS_exceptions


class EntryConfig:

    def __init__(self, dtype, shape, is_numpy, track_name=None, mapped_block_idx=-1):
        """
        Each entry config represents configuration of an entry in one track

        :param dtype: If current entry represents a numpy array, dtype is element type
                      of this numpy array. Otherwise, dtype is type of the object.
        :param shape: If current entry represents a numpy array, shape is the shape of
                      this numpy array. Otherwise, shape is None.
        :param is_numpy: Whether this entry represent a numpy array.
        :param track_name: name of the track associated with this entry
        :param mapped_block_idx: Index of the block in which current entry is stored.
        """
        # data configuration
        self.dtype = dtype
        self.shape = shape
        self.is_numpy = is_numpy

        # management
        self.track_name = track_name
        self.mapped_block_idx = mapped_block_idx
        self.pending_reader_list = []

    def get_value(self):
        """
        This only returns necessary information (i.e. it ignores pending_reader_list which is
        not used by users)
        """
        return EntryConfig(dtype=self.dtype, shape=self.shape, is_numpy=self.is_numpy,
                           track_name=self.track_name, mapped_block_idx=self.mapped_block_idx)


class RWLock:

    def __init__(self):
        """
        This class implements a fair RW lock between multiple readers and writers.
        """
        self.reader_counter_lock = multiprocessing.Lock()
        self.writer_lock = multiprocessing.Lock()
        self.readwrite_lock = multiprocessing.Lock()
        self.reader_count = 0

    def reader_enter(self):
        self.writer_lock.acquire()
        self.reader_counter_lock.acquire()
        if self.reader_count == 0:
            self.readwrite_lock.acquire()
        self.reader_count += 1
        self.reader_counter_lock.release()
        self.writer_lock.release()

    def reader_leave(self):
        self.reader_counter_lock.acquire()
        self.reader_count -= 1
        if self.reader_count == 0:
            self.readwrite_lock.release()
        self.reader_counter_lock.release()

    def writer_enter(self):
        self.writer_lock.acquire()
        self.readwrite_lock.acquire()

    def writer_leave(self):
        self.readwrite_lock.release()
        self.writer_lock.release()


class ConnectionDescriptor:

    def __init__(self, ip, port, authkey):
        """
        Describes the location of a python multiprocessing manager

        :param ip: ip address, could be string 'localhost'
        :param port: 0 - 65535
        :param authkey: a bytes string

        """
        self.ip = ip
        self.port = port
        self.authkey = authkey


class ObjectHandle:

    def __init__(self):
        """
        Each ObjectHandle represents an entry of SharedMemoryObject in remote
        SMOS server.
        """
        # basic information
        self.name = None
        self.entry_idx = None
        self.track_count = None

        # management
        self.entry_config_list = None
        self.shm_list = []
        self.buf_list = []  # this only contains shm.buf, used for clean up


def get_local_free_port(num, low, high):
    """
    Get num free ports between low and high if possible.

    :exception object_store_exceptions.SMOSPortBusy:  If not enough free ports are available

    :param num: number of free ports required
    :param low: lower bound of port idx
    :param high: upper bound of port idx
    :return: free port list
    """
    port_list = []
    for port in range(low, high):
        # check if port is in use
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            is_busy = s.connect_ex(('localhost', port)) == 0
        if not is_busy:
            port_list.append(port)
        if len(port_list) == num:
            return port_list
    raise SMOS_exceptions.SMOSPortBusy(f"Out of free ports (required {num}, available {len(port_list)}).")


def safe_execute(target, args=()):
    """
    Automatically retry when python multiprocessing manager fails due to
    port number drainage. Wrap this outside every remote call to server
    functions.

    :param target: the function to run
    :param args: arguments of target function
    :return: what the function returns
    """
    fail_counter = 0
    while True:
        try:
            return target(*args)
        except TypeError:
            if fail_counter > 10:
                raise TypeError
            else:
                fail_counter += 1
                log2terminal(info_type="Warning", msg=f"Proxy fails {fail_counter} times. Retrying...")


def remove_shm_from_resource_tracker():
    """Monkey-patch multiprocessing.resource_tracker so SharedMemory won't be tracked

    More details at: https://bugs.python.org/issue38119
    """

    def fix_register(name, rtype):
        if rtype == "shared_memory":
            return
        return resource_tracker._resource_tracker.register(self, name, rtype)

    resource_tracker.register = fix_register

    def fix_unregister(name, rtype):
        if rtype == "shared_memory":
            return
        return resource_tracker._resource_tracker.unregister(self, name, rtype)

    resource_tracker.unregister = fix_unregister

    if "shared_memory" in resource_tracker._CLEANUP_FUNCS:
        del resource_tracker._CLEANUP_FUNCS["shared_memory"]


def log2terminal(info_type, msg, worker_type=""):
    """
    log message to terminal in uniform format

    :param info_type: Usually Error / Warning / Info
    :param msg: message body
    :param worker_type: (optional) Identifies who send this message.
    """
    print(f"[(pid={os.getpid()}){worker_type}] {info_type}: {msg}", flush=True)


def serialize_numpy(numpy_array):
    """
    Serialize a numpy array for one track. This is the recommended way of putting a
     numpy array into shared memory object store.

    :exception object_store_exceptions.SMOSInputTypeError: If input is not a numpy array

    :param numpy_array: numpy array to be serialized
    :return: entry_config, numpy_array
    """
    # check if input is a numpy array
    if not type(numpy_array) == np.ndarray:
        raise SMOS_exceptions.SMOSInputTypeError(f"Input not numpy array.")

    # construct entry config for each array
    entry_config = EntryConfig(dtype=numpy_array.dtype, shape=numpy_array.shape, is_numpy=True)
    return entry_config, numpy_array


def serialize_numpy_list(numpy_list):
    """
    Serialize a list of numpy arrays for multiple tracks. This is the recommended
    way of putting a list of numpy arrays into shared memory object store.

    :exception object_store_exceptions.SMOSInputTypeError:  If input is not a list of
                                                            numpy arrays

    :param numpy_list: a list of numpy to be serialized
    :return: entry_config_list, numpy_list
    """
    # check if input is a list of numpy arrays
    if not type(numpy_list) == list:
        raise SMOS_exceptions.SMOSInputTypeError(f"Input not list.")
    if len(numpy_list) == 0:
        return [], []
    if not type(numpy_list[0]) == np.ndarray:
        raise SMOS_exceptions.SMOSInputTypeError(f"Expected numpy list, got list of"
                                                 f" {type(numpy_list[0])}.")

    # construct entry config for each array
    entry_config_list = []
    for np_array in numpy_list:
        entry_config = EntryConfig(dtype=np_array.dtype, shape=np_array.shape, is_numpy=True)
        entry_config_list.append(entry_config)
    return entry_config_list, numpy_list


def deserialize_numpy(entry_config: EntryConfig, offset, buffer):
    """
    Deserialize a numpy array from buffer (zero copy).

    :exception SMOS_exceptions.SMOSInputTypeError: if entry does not contain data of
               a numpy array

    :param entry_config: configuration of target entry
    :param offset: offset in buffer
    :param buffer: buffer that stores the data
    :return: deserialized numpy array
    """
    # check if input is numpy
    if not entry_config.is_numpy:
        raise SMOS_exceptions.SMOSInputTypeError("Current entry is not numpy.")

    # deserialize
    deserialized_array = np.ndarray(shape=entry_config.shape, dtype=entry_config.dtype,
                                    buffer=buffer, offset=offset)
    return deserialized_array


def deserialize_numpy_list(entry_config_list: [EntryConfig], offset_list, buffer_list):
    """
    Deserialize a list of numpy arrays from buffers (zero copy).

    :param entry_config_list: configuration of target entries
    :param offset_list: offset in buffer
    :param buffer_list: buffer that stores the data
    :return: a list of deserialized numpy arrays
    """
    # deserialize
    deserialized_array_list = []
    for idx in range(len(entry_config_list)):
        deserialized_array = deserialize_numpy(entry_config=entry_config_list[idx],
                                               offset=offset_list[idx], buffer=buffer_list[idx])
        deserialized_array_list.append(deserialized_array)

    # return
    return deserialized_array_list


def serialize(obj, file=None):
    """
    This is a general serializer based on pickle. Note that (list of) numpy arrays
    should use serialize_numpy(_list) instead for better performance.

    :param obj: object to be serialized
    :param file: file to write the stream into
    :return: data_stream if file is None
             no return if file is not None
    """
    if file is None:
        data_stream = pickle.dumps(obj=obj, protocol=pickle.HIGHEST_PROTOCOL)
        return data_stream
    else:
        pickle.dump(obj=obj, file=file, protocol=pickle.HIGHEST_PROTOCOL)


def serialize_list(obj_list):
    """
    This is a general serializer based on pickle. Note that (list of) numpy arrays
    should use serialize_numpy(_list) instead for better performance.

    :exception object_store_exceptions.SMOSInputTypeError: If input is not a list.

    :param obj_list: list of objects to be serialized
    :return: data_stream_list
    """
    # check if input is a list
    if not type(obj_list) == list:
        raise SMOS_exceptions.SMOSInputTypeError("Input not list")

    # serialize
    data_stream_list = []
    for obj in obj_list:
        data_stream = serialize(obj)
        data_stream_list.append(data_stream)
    return data_stream_list


def deserialize(data_stream):
    """
    This is a general deserializer based on pickle. Note that data_stream must be
    the result of previous calls to serialize.

    :param data_stream: data stream to be deserialized
    :return: deserialized_object
    """
    deserialized_object = pickle.loads(data=data_stream)
    return deserialized_object


def deserialize_list(data_stream_list):
    """
    This is a general deserializer based on pickle. Note that data_stream_list must be
    the result of previous calls to serialize_list.

    :exception object_store_exceptions.SMOSInputTypeError: If input is not a list.

    :param data_stream_list: list of data streams to be deserialized
    :return: deserialized_object_list
    """
    # check if input is a list
    if not type(data_stream_list) == list:
        raise SMOS_exceptions.SMOSInputTypeError("Input not list")

    # deserialize
    deserialized_object_list = []
    for data_stream in data_stream_list:
        deserialized_object_list.append(deserialize(data_stream))
    return deserialized_object_list
