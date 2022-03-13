"""
2021.12.04 Yixuan Mei
This file is used to test SMOS correctness in single process case.
"""
import pickle

import SMOS
import SMOS_utils
import numpy as np


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

    """"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

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

    """"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

    # 4: fined-grained operation test for numpy
    # print("fined-grained test")
    #
    # # write
    # data = np.random.randn(10)
    # client.create_object(name="obj1", max_capacity=2, track_count=1, block_size=512)
    # _, handle = client.create_entry(name="obj1", dtype=data.dtype, shape=data.shape, is_numpy=True)
    # _, buffer_list = client.open_shm(handle)
    # buffer_list[0][:] = data[:]
    # _, idx = client.commit_entry(handle)
    # print(f"idx {idx}")
    #
    # # read
    # _, handle = client.open_entry(name="obj1", entry_idx=0)
    # _, buffer_list = client.open_shm(handle)
    # print(buffer_list[0])
    # client.release_entry(handle)
    #
    # # another read
    # status, object_handle, obj = client.read_from_object(name="obj1", entry_idx=0)
    # print(status, obj)
    # client.release_entry(object_handle)
    #
    # # yet another read
    # status, object_handle, obj = client.pop_from_object(name="obj1")
    # print(status, obj)
    # client.free_handle(object_handle)
    #
    # # delete
    # client.delete_entry(name="obj1", entry_idx=1)

    """"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

    # 5: fine-grained operation test for non-numpy
    # print("fined-grained test")
    #
    # # write
    # data = SMOS_utils.EntryConfig(1, (1, 2), False)
    # client.create_object(name="obj1", max_capacity=2, track_count=1, block_size=512)
    # _, handle = client.create_entry(name="obj1", dtype=None, shape=None, is_numpy=False)
    # _, buffer_list = client.open_shm(handle)
    # SMOS.serialize(obj=data, buf=buffer_list[0])
    # _, idx = client.commit_entry(handle)
    # print(f"idx {idx}")
    #
    # # read
    # _, handle = client.open_entry(name="obj1", entry_idx=0)
    # _, buffer_list = client.open_shm(handle)
    # obj0 = SMOS.deserialize(buffer_list[0])
    # print(obj0)
    # client.release_entry(handle)
    #
    # # another read
    # status, object_handle, obj1 = client.read_from_object(name="obj1", entry_idx=0)
    # print(status, obj1)
    # client.release_entry(object_handle)
    #
    # # yet another read
    # status, object_handle, obj2 = client.pop_from_object(name="obj1")
    # print(status, obj2)
    # client.free_handle(object_handle)
    #
    # # delete
    # client.delete_entry(name="obj1", entry_idx=0)

    """"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

    # 6: test put
    # print("put and get")
    # data = [SMOS_utils.EntryConfig(1, (1, 2), False), 124, np.random.randn(10)]
    # client.put(name="obj1", data=data, as_list=True)
    # print("put finished")
    #
    # # use pop to read
    # status, object_handle, obj = client.pop_from_object(name="obj1")
    # print(status, obj)
    # if status == SMOS.SMOS_SUCCESS:
    #     client.free_handle(object_handle)
    #
    # status, object_handle, obj = client.pop_from_object(name="obj1")
    # print(status, obj)
    # if status == SMOS.SMOS_SUCCESS:
    #     client.free_handle(object_handle)
    #
    # status, object_handle, obj = client.pop_from_object(name="obj1")
    # print(status, obj)
    # if status == SMOS.SMOS_SUCCESS:
    #     client.free_handle(object_handle)

    """"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

    # 7: test get
    # data = [124, np.random.randn(10), SMOS_utils.EntryConfig(1, (1, 2), False)]
    # client.put(name="obj1", data=data)
    # print(client.get(name="obj1"))
    # print(client.get(name="obj1"))

    """"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

    # # 8: queue test 2
    # print("Queue test")
    # client.create_object(name="obj1", max_capacity=4, track_count=1, block_size=128)
    #
    # # push
    # data1 = [[123, 456, 789]]
    # client.push_to_object(name="obj1", data=[data1])
    # data2 = [[123123, 456, 789, 23]]
    # client.push_to_object(name="obj1", data=[data2])
    #
    # # pop
    # status, object_handle, obj1 = client.pop_from_object(name="obj1")
    # print(status, obj1)
    # client.release_entry(object_handle)
    #
    # status, object_handle, obj2 = client.pop_from_object(name="obj1")
    # print(status, obj2)
    # client.release_entry(object_handle)
    #
    # client.remove_object(name="obj1")

    """"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

    # 8: queue test 3
    # print("Queue test")
    # client.create_object(name="obj1", max_capacity=4, track_count=1, block_size=128)
    #
    # # push
    # data1 = np.array([[b'1', b'1'], [b'1', b'1']])
    # print(client.push_to_object(name="obj1", data=[data1]))
    # data1 = np.array([[b'222', b'222'], [b'222', b'222']])
    # print(client.push_to_object(name="obj1", data=[data1]))
    # data1 = np.array([[b'33', b'33'], [b'33', b'33']])
    # print(client.push_to_object(name="obj1", data=[data1]))
    # print(client.get_entry_count("obj1")[1])
    #
    # # read batch
    # status, handle_batch, data_batch = client.batch_read_from_object(name="obj1", entry_idx_batch=[0, 1, 2])
    # if status == SMOS.SMOS_SUCCESS:
    #     print(data_batch)
    #     for handle, data in zip(handle_batch, data_batch):
    #         print(data)
    #         client.release_entry(object_handle=handle)
    # else:
    #     print("read failed")
    #
    # # delete
    # print(client.delete_entry(name="obj1", entry_idx=1))
    #
    # # read batch
    # status, handle_batch, data_batch = client.batch_read_from_object(name="obj1", entry_idx_batch=[1, 2])
    # if status == SMOS.SMOS_SUCCESS:
    #     print(data_batch)
    #     for handle, data in zip(handle_batch, data_batch):
    #         print(data)
    #         client.free_handle(object_handle=handle)
    # else:
    #     print("read failed as expected")
    #
    # # remove
    # client.remove_object(name="obj1")

    """"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

    # print("Read latest test")
    # client.create_object(name="obj1", max_capacity=4, track_count=1, block_size=128)
    #
    # # read from empty
    # print(client.read_latest_from_object(name="obj1"))
    #
    # # push 1
    # print(client.push_to_object(name="obj1", data=[1]))
    # print(client.push_to_object(name="obj1", data=[2]))
    # print(client.push_to_object(name="obj1", data=[3]))
    #
    # # read
    # status, handle, data = client.read_latest_from_object(name="obj1")
    # print(data)
    # client.release_entry(object_handle=handle)
    #
    # # push 2
    # print(client.push_to_object(name="obj1", data=[4]))
    #
    # # read
    # status, handle, data = client.read_latest_from_object(name="obj1")
    # print(data)
    # client.release_entry(object_handle=handle)
    #
    # # delete
    # print(client.delete_entry(name="obj1", entry_idx=1))
    #
    # # read
    # status, handle, data = client.read_latest_from_object(name="obj1")
    # print(data)
    # client.release_entry(object_handle=handle)
    #
    # # delete
    # print(client.delete_entry(name="obj1", entry_idx=3))
    #
    # # read
    # status, handle, data = client.read_latest_from_object(name="obj1")
    # print(data)
    # client.release_entry(object_handle=handle)
    #
    # print(client.push_to_object(name="obj1", data=[-10]))
    # status, handle, data = client.read_latest_from_object(name="obj1")
    # print(data)
    # client.release_entry(object_handle=handle)
    #
    # # remove
    # client.remove_object(name="obj1")

    """"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

    # print("Read latest test")
    # client.create_object(name="obj1", max_capacity=4, track_count=1, block_size=128)
    #
    # # push 1
    # client.push_to_object(name="obj1", data=[1])
    # client.push_to_object(name="obj1", data=[2])
    # client.push_to_object(name="obj1", data=[3])
    #
    # # read
    # status, handle1, data1 = client.read_latest_from_object(name="obj1")
    # status, handle2, data2 = client.read_latest_from_object(name="obj1")
    # print(data1, data2)
    # client.release_entry(object_handle=handle1)
    # client.push_to_object(name="obj1", data=[3])
    # client.release_entry(object_handle=handle2)
    #
    # # remove
    # client.remove_object(name="obj1")

    """"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

    # print("Read latest test")
    # client.create_object(name="obj1", max_capacity=4, track_count=1, block_size=128)
    #
    # # push 1
    # client.push_to_object(name="obj1", data=[1])
    #
    # # read
    # status, handle1, data1 = client.read_latest_from_object(name="obj1")
    # status, handle2, data2 = client.read_from_object(name="obj1", entry_idx=0)
    # print(data1, data2)
    # client.release_entry(object_handle=handle1)
    # client.release_entry(object_handle=handle2)
    # print(client.delete_entry(name="obj1", entry_idx=0))
    #
    # # remove
    # client.remove_object(name="obj1")

    """"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

    print("Batch read test")
    client.create_object(name="obj1", max_capacity=8, track_count=1, block_size=128)

    # push 1
    client.push_to_object(name="obj1", data=[0])
    client.push_to_object(name="obj1", data=[1])
    client.push_to_object(name="obj1", data=[2])
    client.push_to_object(name="obj1", data=[3])

    # read
    status, handle_list, data_list = client.batch_read_from_object(name="obj1", entry_idx_batch=[0, 1, 3])
    print(data_list)
    for handle in handle_list:
        client.release_entry(object_handle=handle)
    delete_status = client.delete_entry(name="obj1", entry_idx=1) == SMOS.SMOS_SUCCESS
    print(f"Delete succeed: {delete_status}")

    # read again
    status, handle_list, data_list = client.batch_read_from_object(name="obj1", entry_idx_batch=[0, 1, 3])
    print(f"Read again succeed: {status == SMOS.SMOS_SUCCESS}")
    delete_status = client.delete_entry(name="obj1", entry_idx=0) == SMOS.SMOS_SUCCESS
    print(f"Delete succeed: {delete_status}")

    # remove
    client.remove_object(name="obj1")

    """"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

    # clean up
    print("test finished")
    server.stop()


if __name__ == '__main__':
    main()
