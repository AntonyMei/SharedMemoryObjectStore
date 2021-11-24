"""
2021.11.24 Yixuan Mei
This file contains function tests for Shared Memory Object Store
"""
import time
import numpy as np
import object_store_util


def main():
    start = time.time()
    a = np.random.randn(40 * 1000 * 1000)
    # b = np.random.randn(40 * 1000 * 1000)

    # c = a + b
    end = time.time()
    print(end - start)


if __name__ == '__main__':
    main()
