"""
2021.12.04 Yixuan Mei
This file is used to test SMOS correctness in single process case.
"""
import multiprocessing as mp
import random
import time

import SMOS
import numpy as np

reader_count = 36
writer_count = 36
shm_count = 6
object_count = 200
object_entry_count = 50 * 1024 * 1024


def writer(idx, address, name):
    client = SMOS.Client(connection=address)
    data = np.ones(object_entry_count) * object_count * idx
    for i in range(object_count):
        status, _ = client.push_to_object(name=name, data=data)
        if status == SMOS.SMOS_SUCCESS:
            print(f"w {int(data[0])}", flush=True)
        else:
            print(f"[write fail ({idx})]", flush=True)
            time.sleep(1)
        data[0] += 1


def reader(idx, address, name):
    client = SMOS.Client(connection=address)
    for i in range(object_count):
        status, handle, data = client.pop_from_object(name=name)
        if status == SMOS.SMOS_SUCCESS:
            print(f"r {int(data[0])}", flush=True)
            client.free_handle(handle)
        else:
            print(f"[read fail ({idx})]", flush=True)
            time.sleep(random.random() * 5)


def main():
    # start server and create object
    server = SMOS.Server()
    address = server.address()
    server.start()

    # create object
    client = SMOS.Client(address)
    for idx in range(shm_count):
        client.create_object(name=f"obj{idx}", max_capacity=64, track_count=1,
                             block_size=object_entry_count * 8)

    # prepare workers
    writers, readers = [], []
    for idx in range(writer_count):
        writer_proc = mp.Process(target=writer, args=(idx, address, f"obj{idx % shm_count}"))
        writers.append(writer_proc)
    for idx in range(reader_count):
        reader_proc = mp.Process(target=reader, args=(idx, address, f"obj{idx % shm_count}"))
        readers.append(reader_proc)

    # launch workers
    print(f"***************************** Max timer starts *****************************", flush=True)
    start_max = time.time()
    for worker in writers + readers:
        worker.start()
    print(f"***************************** Pure timer starts *****************************", flush=True)
    start_pure = time.time()

    end_pure = 0
    timer_flag = False
    while True:
        time.sleep(10)
        alive_count = 0
        for worker in writers + readers:
            if worker.is_alive():
                alive_count += 1
        print(f"***************************** Alive workers: {alive_count} *****************************",
              flush=True)
        if alive_count < (reader_count + writer_count) / 2:
            if not timer_flag:
                print(f"***************************** Pure timer ends *****************************", flush=True)
                end_pure = time.time()
                timer_flag = True
        if alive_count == 0:
            print(f"***************************** Max timer ends *****************************", flush=True)
            end_max = time.time()
            break

    # clean up
    for worker in writers + readers:
        worker.join()
    server.stop()

    # log
    max_time = end_max - start_max
    pure_time = end_pure - start_pure
    data_size = writer_count * object_count * object_entry_count * 8 / 1024 / 1024 / 1024
    print(f"***************************** Result *****************************", flush=True)
    print(f"[Perf upper bound]\n"
          f"Time: {pure_time}s\n"
          f"Write: {data_size / pure_time} GB/s\n"
          f"Throughput: {2 * data_size / pure_time} GB/s")
    print(f"[Perf lower bound]\n"
          f"Time: {max_time}s\n"
          f"Write: {data_size / max_time} GB/s\n"
          f"Throughput: {2 * data_size / max_time} GB/s")
    print(f"******************************************************************", flush=True)


if __name__ == '__main__':
    main()
