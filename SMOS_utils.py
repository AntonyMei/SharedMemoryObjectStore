"""
2021.11.24 Yixuan Mei
This file contains utility functions for Shared Memory Object Store
"""

import os
import multiprocessing
import pickle
import socket

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
        self.pending_readers = 0


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

    def get_ip(self):
        return self.ip

    def get_port(self):
        return self.port

    def get_authkey(self):
        return self.authkey


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


def log2terminal(info_type, msg, worker_type=""):
    """
    log message to terminal in uniform format

    :param info_type: Usually Error / Warning / Info
    :param msg: message body
    :param worker_type: (optional) Identifies who send this message.
    """
    print(f"[(pid={os.getpid()}){worker_type}] {info_type}: {msg}")


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


def serialize(obj):
    """
    This is a general serializer based on pickle. Note that (list of) numpy arrays
    should use serialize_numpy(_list) instead for better performance.

    :param obj: object to be serialized
    :return: entry_config, data_stream
    """
    entry_config = EntryConfig(dtype=type(obj), shape=None, is_numpy=False)
    data_stream = pickle.dumps(obj=obj, protocol=pickle.HIGHEST_PROTOCOL)
    return entry_config, data_stream


def serialize_list(obj_list):
    """
    This is a general serializer based on pickle. Note that (list of) numpy arrays
    should use serialize_numpy(_list) instead for better performance.

    :exception object_store_exceptions.SMOSInputTypeError: If input is not a list.

    :param obj_list: list of objects to be serialized
    :return: entry_config_list, data_stream_list
    """
    # check if input is a list
    if not type(obj_list) == list:
        raise SMOS_exceptions.SMOSInputTypeError("Input not list")

    # serialize
    entry_config_list = []
    data_stream_list = []
    for obj in obj_list:
        entry_config, data_stream = serialize(obj)
        entry_config_list.append(entry_config)
        data_stream_list.append(data_stream)
    return entry_config_list, data_stream_list


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