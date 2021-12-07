"""
2021.12.04 Yixuan Mei
This file is used to test SMOS correctness in single process case.
"""
import time

import SMOS
import numpy as np

import multiprocessing.shared_memory as shared_memory


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

    # 2: queue API ()
    client.create_object(name="obj1", max_capacity=4, track_count=3, block_size=[128, 128, 128])
    client.push_to_object(name="obj1", data=[123, 456, 789])
    client.remove_object(name="obj1")

    # clean up
    print("test finished")
    server.stop()


if __name__ == '__main__':
    main()
