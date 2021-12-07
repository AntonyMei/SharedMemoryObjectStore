"""
2021.12.04 Yixuan Mei
This file is used to test SMOS correctness in single process case.
"""
import time

import SMOS
import numpy as np

import multiprocessing.shared_memory as shared_memory

import SMOS_utils


class TestClass:

    def __init__(self, name, lst):
        self.name = name
        self.list = lst

    def print(self):
        print(self.name, self.list)


def main():

    # start server
    server = SMOS.Server()
    server_address = server.address()
    server.start()

    # get client
    client = SMOS.Client(connection=server_address)
    print("start test")

    # 1: create & remove
    # client.create_object(name="obj1", max_capacity=4, track_count=3, block_size=[128, 128, 128])
    # client.remove_object(name="obj1")

    """"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

    # 2: queue API (push_to_object & pop_from_object  ->  free_handle)
    # client.create_object(name="obj1", max_capacity=4, track_count=3, block_size=[128, 130, 132])
    #
    # client.push_to_object(name="obj1", data=[np.ones(5), np.ones(5), 234])
    # status, object_handle, obj = client.pop_from_object(name="obj1")
    # print(status, obj)
    # client.free_handle(object_handle)
    #
    # client.remove_object(name="obj1")

    """"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

    # 3: queue API (push_to_object & read_from_object ->  release_entry)
    # print("Read test")
    # client.create_object(name="obj1", max_capacity=4, track_count=3, block_size=[128, 130, 512])
    #
    # client.push_to_object(name="obj1", data=[np.ones(5), np.ones(5), SMOS_utils.EntryConfig(1, (1, 2), False)])
    # status, object_handle, obj = client.read_from_object(name="obj1", entry_idx=0)
    # print(status, obj)
    # client.release_entry(object_handle)
    #
    # client.remove_object(name="obj1")
    #
    # # clean up
    # print("test finished")
    # server.stop()

    """"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""


if __name__ == '__main__':
    main()
