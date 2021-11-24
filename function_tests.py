"""
2021.11.24 Yixuan Mei
This file contains function tests for Shared Memory Object Store
"""
import time
import numpy as np
import object_store_util


def main():
    start = time.time()

    a = [np.ones(3), np.ones(3)]
    print(object_store_util.serialize_numpy_list(a))

    end = time.time()
    print(end - start)


if __name__ == '__main__':
    main()
