"""
2021.11.25 Yixuan Mei
This file contains definition of class DataTrack for Shared Memory Object Store
"""

import queue
from multiprocessing import shared_memory

import object_store_exceptions
import object_store_util as util


class DataTrack:

    def __init__(self, track_name, shm_name, block_size, max_capacity):
        """
        Each data track manages a shared memory space. Can be used as a buffer or queue.
        Note that data tracks are components of SharedMemoryObject. Therefore, we do not
        handle concurrency issues at this level. All locks are imposed on SharedMemoryObject.

        :param track_name: name of this data track
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
        self.shm = shared_memory.SharedMemory(create=True, size=block_size * max_capacity, name=shm_name)

        # array management
        self.entry_config_list = []
        self.free_block_list = queue.Queue(maxsize=max_capacity)
        for i in range(max_capacity):
            self.free_block_list.put(i)

    def allocate_block(self, entry_config: util.EntryConfig):
        """
        Allocate a free block for a new entry and write into entry config

        :param entry_config: configuration of new entry
        :return: [0, entry_config] if successful, [-1, entry_config] if no free block available
        """
        try:
            block_idx = self.free_block_list.get(block=False)
            entry_config.mapped_block_idx = block_idx
            return 0, entry_config
        except queue.Empty:
            return -1, entry_config

    def append_entry_config(self, entry_config: util.EntryConfig):
        """
        Append configuration of new entry to this data track's configuration list

        :exception object_store_exceptions.SMOSEntryUnallocated: if entry_config is mapped to -1 block

        :param entry_config: configuration of new entry
        :return: always 0
        """
        # check if entry has been allocated
        if entry_config.mapped_block_idx == -1:
            raise object_store_exceptions.SMOSEntryUnallocated(f"Entry unallocated.")

        # append entry config
        self.entry_config_list.append(entry_config)
        return 0
