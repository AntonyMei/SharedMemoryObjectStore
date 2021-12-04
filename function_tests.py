"""
2021.11.24 Yixuan Mei
This file contains function tests for Shared Memory Object Store
"""
import time
import numpy as np
import SMOS_server
import SMOS_utils
import SMOS_data_track as dt
import pickle
import SMOS_exceptions
import multiprocessing as mp
from multiprocessing import shared_memory


def put(data, as_list=False):
    """
    Create a SharedMemoryObject with name,  and put data into this SharedMemoryObject.

    :param name: name of share memory object
    :param data: data to be put into SMOS
    :param as_list: If as_list = True and data is a list, then each element of the list
           will be stored in a single entry in the SharedMemoryObject. In other cases,
           data will be stored as a whole in one entry and the SharedMemoryObject will
           have only one entry.
    :param redundancy: Require redundancy number of extra free entries in new
           SharedMemoryObject. These extra entries can be used by entry operations.
    :return:
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
    print(data, entry_count, track_count)


def func1():
    shm = shared_memory.SharedMemory(name="Test")
    res = np.ndarray(shape=(5, ), dtype=int, buffer=shm.buf)
    print(res)
    res2 = res.copy()
    return res2

def remote_test():
    print("123")
    res= func1()
    print(res)
    pass

class test_wrap:

    def __init__(self, data):
        self.data = data

def main():
    start = time.time()

    # a = np.random.randn(40 * 1024 * 1024)
    # b = np.random.randn(40 * 1024 * 1024)
    # a = [np.ones(3), np.ones(4)]
    # res = object_store_util.serialize_numpy_list(a)
    # print(res)
    # a = np.ones(3)
    # res = object_store_util.serialize_numpy(a)
    # print(res)
    # ec1 = object_store_util.EntryConfig(dtype=2123, shape=[np.ones(3), np.zeros(4), 1, "abc"])
    # ec2 = object_store_util.EntryConfig(dtype=2123, shape=ec1)
    # config_list, stream_list = object_store_util.serialize([ec1, ec2])
    # res = object_store_util.deserialize(stream_list)
    # print(res)
    # a = dt.DataTrack("abc", "abc", 1024, 10)
    # b = SMOS_utils.EntryConfig(1, 1, True)
    # a.allocate_block(b)
    # a.stop()
    # print(b.mapped_block_idx)

    """"""""""""""""""""""""""""""""""""""""""""""""
    # test merge byte arrays
    # test_obj = SMOS_utils.EntryConfig(dtype=1, shape=2, is_numpy=True)
    # _, stream = SMOS_utils.serialize(test_obj)
    # stream = bytearray(stream)
    # stream2 = bytearray(b"abcdeadf")
    # c = stream + stream2
    # c = bytes(c)
    # res = SMOS_utils.deserialize(c)
    # print(res)
    """"""""""""""""""""""""""""""""""""""""""""""""

    # test SMOS
    # server = SMOS_server.Server()
    # server.start()
    # address = server.address()
    # print(address.ip, address.port, address.authkey)
    # server.stop()
    """"""""""""""""""""""""""""""""""""""""""""""""

    # test write into shm
    # create object
    obj = SMOS_utils.EntryConfig(dtype=int, shape=(3,0), is_numpy=True)
    serialized = pickle.dumps(obj=obj, protocol=pickle.HIGHEST_PROTOCOL)

    # put into shared memory
    shm = shared_memory.SharedMemory(create=True, size=4096, name="Test")
    shm.buf[0:len(serialized)] = serialized

    # deserialize
    deserialize_stream = shm.buf[0:len(serialized)]
    recovered = SMOS_utils.deserialize(deserialize_stream)
    b = test_wrap(deserialize_stream)
    # b.data.release()
    deserialize_stream.release()
    proc = mp.Process(target=remote_test)
    proc.start()
    proc.join()
    shm.unlink()

    """"""""""""""""""""""""""""""""""""""""""""""""
    # put(1, False)
    # put(1, True)
    # put([1, 2, 3], False)
    # put([1, 2, 4], True)
    # put([[1, 1], [2, 2], [3, 3]], False)
    # put([[1, 1], [2, 2], [3, 3]], True)
    # put([[1, 1], [2, 2], [3, 3, 4]], False)

    """"""""""""""""""""""""""""""""""""""""""""""""

    end = time.time()
    print(f"time:{end - start}")


if __name__ == '__main__':
    main()
