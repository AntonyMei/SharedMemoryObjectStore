"""
2021.12.04 Yixuan Mei
This file is used to test SMOS correctness in multiprocessing.
"""
import multiprocessing as mp
import random
import time

import SMOS
import numpy as np

# reader
reader_count = 16

# smos
object_count = 16
sample_batch_size = 128
object_entry_count = 12000
string_length = 2048


def build_random_string(size, string_length):
    res = []
    for _ in range(size):
        res.append(np.random.bytes(string_length))
    return np.array(res)


def reader(worker_idx, address):
    # connect
    print(f"[reader up r{worker_idx}]", flush=True)
    client = SMOS.Client(connection=address)
    print(f"[reader connected r{worker_idx}]", flush=True)

    # load ground truth
    ground_truth_list = []
    for idx in range(object_count):
        ground_truth_1 = np.load(f"{idx}_1.npy")
        ground_truth_2 = np.load(f"{idx}_2.npy")
        ground_truth_list.append([ground_truth_1, ground_truth_2])

    # read
    pass_batch_count = 0
    while True:
        # prepare idx batch
        idx_batch = [np.random.randint(0, object_count) for _ in range(sample_batch_size)]
        status, object_handle_batch, reconstructed_object_batch = \
            client.batch_read_from_object(name="obj", entry_idx_batch=idx_batch)

        # check equality
        has_error = False
        for idx, reconstructed_object in zip(idx_batch, reconstructed_object_batch):
            res_1 = reconstructed_object[0] == ground_truth_list[idx][0]
            res_2 = reconstructed_object[1] == ground_truth_list[idx][1]
            res = np.hstack((res_1, res_2))
            if not len(set(res)) == 1 or not res[0]:
                print(f"[worker {worker_idx}] Find error!")
                # print(ground_truth_list[idx])
                # print(len(set(res)), res[0])
                # print(reconstructed_object)
                has_error = True

        # log
        if not has_error:
            pass_batch_count += 1
            if pass_batch_count % 100 == 0:
                print(f"[worker {worker_idx}] Passed {pass_batch_count} batches.")

        # cleanup
        for object_handle in object_handle_batch:
            client.release_entry(object_handle=object_handle)


def main():
    # start server and create object
    server = SMOS.Server()
    address = server.address()
    server.start()

    # create object
    client = SMOS.Client(address)
    client.create_object(name=f"obj", max_capacity=1024, track_count=2,
                         block_size=[object_entry_count * 8 * 2, object_entry_count * string_length * 16])
    for idx in range(object_count):
        print(f"Constructing data {idx}")
        data1 = np.random.randn(object_entry_count)
        data2 = build_random_string(object_entry_count, string_length)
        client.push_to_object(name="obj", data=[data1, data2])
        np.save(file=f"{idx}_1.npy", arr=data1)
        np.save(file=f"{idx}_2.npy", arr=data2)

    # prepare workers
    readers = []
    for idx in range(reader_count):
        reader_proc = mp.Process(target=reader, args=(idx, address,))
        readers.append(reader_proc)

    # launch workers
    for worker in readers:
        worker.start()
        time.sleep(0.1)

    # clean up
    for worker in readers:
        worker.join()
    server.stop()


if __name__ == '__main__':
    main()
