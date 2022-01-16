"""
2022.01.16 Yixuan Mei
This file requires SMOS_antony package.
"""
import multiprocessing as mp
import time

import SMOS

reader_count = 36
writer_count = 36


def writer(idx):
    print(f"[writer up w{idx}]", flush=True)
    print(f"[writer connected w{idx}]", flush=True)


def reader(idx):
    print(f"[reader up r{idx}]", flush=True)
    print(f"[reader connected r{idx}]", flush=True)


def main():
    # start a server in another process
    server = SMOS.Server()
    server.start()

    # prepare workers
    writers, readers = [], []
    for idx in range(writer_count):
        writer_proc = mp.Process(target=writer, args=(idx,))
        writers.append(writer_proc)
    for idx in range(reader_count):
        reader_proc = mp.Process(target=reader, args=(idx,))
        readers.append(reader_proc)

    # launch workers
    for worker in writers + readers:
        worker.start()

    while True:
        time.sleep(5)

        # check readers
        reader_alive_count = 0
        reader_alive_list = []
        for idx in range(len(readers)):
            worker = readers[idx]
            if worker.is_alive():
                reader_alive_count += 1
                reader_alive_list.append(idx)

        # check writers
        writer_alive_count = 0
        writer_alive_list = []
        for idx in range(len(writers)):
            worker = writers[idx]
            if worker.is_alive():
                writer_alive_count += 1
                writer_alive_list.append(idx)

        # log alive workers
        alive_count = reader_alive_count + writer_alive_count
        print(f"***************************** Alive readers: {reader_alive_count} *****************************",
              flush=True)
        print(reader_alive_list, flush=True)
        print(f"***************************** Alive writers: {writer_alive_count} *****************************",
              flush=True)
        print(writer_alive_list, flush=True)

        # break watchdog when all workers exit
        if alive_count == 0:
            break

    # clean up
    for worker in writers + readers:
        worker.join()
    server.stop()


if __name__ == '__main__':
    main()
