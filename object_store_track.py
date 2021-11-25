"""
2021.11.25 Yixuan Mei
This file contains definition of class Track for Shared Memory Object Store
"""

import queue
import multiprocessing as mp


class Track:

    def __init__(self, track_name, shm_name, block_size, max_capacity):
        """
        Each track manages a shared memory space. Can be used as a buffer or queue.
        Note that track is a component of SharedMemoryObject. Therefore, it does not
        handle concurrency issues. All locks are imposed on SharedMemoryObject level.

        :param track_name: name of this track
        :param shm_name: name of the underlying shared memory space
        :param block_size: size of each block
        :param max_capacity: maximum number of objects that can be stored in this track
        """

        # parameters
        self.track_name = track_name
        self.shm_name = shm_name
        self.block_size = block_size
        self.max_capacity = max_capacity

        # underlying shared memory space
        self.shm = mp.shared_memory.SharedMemory(create=True, size=block_size * max_capacity, name=shm_name)

        # array management
        self.entry_config_list = []
        self.free_block_list = queue.Queue(maxsize=max_capacity)
        for i in range(max_capacity):
            self.free_block_list.put(i)
