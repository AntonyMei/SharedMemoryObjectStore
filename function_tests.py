"""
2021.11.24 Yixuan Mei
This file contains function tests for Shared Memory Object Store
"""
import time
import numpy as np
import object_store_util


def main():
    start = time.time()

    # a = np.random.randn(40 * 1024 * 1024)
    # b = np.random.randn(40 * 1024 * 1024)
    # a = [np.ones(3), np.ones(4)]
    # res = object_store_util.serialize_numpy_list(a)
    # print(res)
    a = np.ones(3)
    res = object_store_util.serialize_numpy(a)
    print(res)

    end = time.time()
    print(end - start)


if __name__ == '__main__':
    main()
