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

import multiprocessing as mp
from multiprocessing import shared_memory

def remote_test():
    pass


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
    del deserialize_stream
    shm.unlink()

    end = time.time()
    print(f"time:{end - start}")


if __name__ == '__main__':
    main()
