"""
2022.01.16 Yixuan Mei
This file requires SMOS_antony package.
"""
import multiprocessing as mp
import time

import SMOS_exceptions

import SMOS

writer_count = 8
total = 8 * 1024 ** 2


def writer(idx, address):
    count = 0
    smos_client = SMOS.Client(address)
    while True:
        _, sync_handle, data = smos_client.read_latest_from_object(name="sync")
        try:
            smos_client.release_entry(object_handle=sync_handle)
        except SMOS_exceptions.SMOSReadRefDoubleRelease:
            print(f"{sync_handle.entry_idx} {idx}******************* {data}")
            raise TypeError
        count += 1
        smos_client.push_to_object(name="sync", data=f"{count} {idx}")
        if count % 1000 == 0:
            print(f"Worker [{idx}] active {count}")
        if count == total / writer_count - 100:
            return


# def reader(idx):
#     print(f"[reader up r{idx}]", flush=True)
#     print(f"[reader connected r{idx}]", flush=True)


def main():
    # start a server in another process
    server = SMOS.Server()
    server.start()
    client = SMOS.Client(server.address())
    client.create_object(name="sync", max_capacity=total, track_count=1, block_size=128)
    client.push_to_object(name="sync", data=0)
    print("initialized")

    # prepare workers
    writers, readers = [], []
    for idx in range(writer_count):
        writer_proc = mp.Process(target=writer, args=(idx, server.address()))
        writers.append(writer_proc)
    # for idx in range(reader_count):
    #     reader_proc = mp.Process(target=reader, args=(idx,))
    #     readers.append(reader_proc)

    # launch workers
    for worker in writers:
        worker.start()
        time.sleep(0.1)

    # while True:
    #     time.sleep(5)
    #
    #     # check readers
    #     reader_alive_count = 0
    #     reader_alive_list = []
    #     for idx in range(len(readers)):
    #         worker = readers[idx]
    #         if worker.is_alive():
    #             reader_alive_count += 1
    #             reader_alive_list.append(idx)
    #
    #     # check writers
    #     writer_alive_count = 0
    #     writer_alive_list = []
    #     for idx in range(len(writers)):
    #         worker = writers[idx]
    #         if worker.is_alive():
    #             writer_alive_count += 1
    #             writer_alive_list.append(idx)
    #
    #     # log alive workers
    #     alive_count = reader_alive_count + writer_alive_count
    #     print(f"***************************** Alive readers: {reader_alive_count} *****************************",
    #           flush=True)
    #     print(reader_alive_list, flush=True)
    #     print(f"***************************** Alive writers: {writer_alive_count} *****************************",
    #           flush=True)
    #     print(writer_alive_list, flush=True)
    #
    #     # break watchdog when all workers exit
    #     if alive_count == 0:
    #         break

    # clean up
    for worker in writers + readers:
        worker.join()
    server.stop()


if __name__ == '__main__':
    main()
