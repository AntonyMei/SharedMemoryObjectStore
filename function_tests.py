"""
2021.11.24 Yixuan Mei
This file contains function tests for Shared Memory Object Store
"""
import time
import numpy as np
import object_store_util
import object_store_data_track as dt


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
    a = dt.DataTrack("abc", "abc", 1024, 10)
    b = object_store_util.EntryConfig(1, 1, True)
    a.allocate_block(b)
    a.stop()
    print(b.mapped_block_idx)

    end = time.time()
    print(end - start)


if __name__ == '__main__':
    main()
