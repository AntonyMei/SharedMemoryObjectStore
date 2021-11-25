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

    # write
    def allocate_block(self, entry_config: util.EntryConfig):
        """
        Allocate a free block for a new entry and write into entry config

        :param entry_config: configuration of new entry
        :return: [0, entry_config] if successful, [-1, entry_config] if no free block available
        """
        try:
            block_idx = self.free_block_list.get(block=False)
            entry_config.mapped_block_idx = block_idx
            entry_config.track_name = self.track_name
            return 0, entry_config
        except queue.Empty:
            return -1, entry_config

    def append_entry_config(self, entry_config: util.EntryConfig):
        """
        Append configuration of new entry to this data track's configuration list

        :exception object_store_exceptions.SMOSEntryUnallocated: if entry_config is mapped to -1 block
        :exception object_store_exceptions.SMOSTrackMismatch: if track_name of current track is different
                   from track name of input entry_config

        :param entry_config: configuration of new entry
        :return: always 0
        """
        # check if entry_config has been correctly allocated
        if entry_config.mapped_block_idx == -1:
            raise object_store_exceptions.SMOSEntryUnallocated(f"Entry unallocated.")
        if not entry_config.track_name == self.track_name:
            raise object_store_exceptions.SMOSTrackMismatch(f"Current track is {self.track_name}, while "
                                                            f"input entry_config is associated with track"
                                                            f" {entry_config.track_name}.")

        # append entry config
        self.entry_config_list.append(entry_config)
        return 0

    # read
    def read_entry_config(self, idx):
        """
        Return entry config at given index and add read reference to that entry.

        :param idx: index of entry config to be returned
        :return: [0, entry_config] if successful, [-1, None] if index out of range
        """
        try:
            self.entry_config_list[idx].pending_readers += 1
            entry_config = self.entry_config_list[idx]
            return 0, entry_config
        except IndexError:
            return -1, None

    def release_read_reference(self, idx):
        """
        Release read reference on given entry.

        :exception object_store_exceptions.SMOSReadRefDoubleRelease: if a read reference is
                   released multiple times.

        :param idx: index of entry to be released
        :return: 0 if successful, -1 if index out of range
        """
        try:
            self.entry_config_list[idx].pending_readers -= 1
            if self.entry_config_list[idx].pending_readers < 0:
                raise object_store_exceptions.SMOSReadRefDoubleRelease(f"Double release on track {self.track_name}"
                                                                       f"index {idx}")
            return 0
        except IndexError:
            return -1

    # delete
    def delete_entry_config(self, idx, force_delete=False):
        """
        Delete an entry from current data track. Note that this is lazy delete, the actual
        data in shared memory is not erased.

        :exception object_store_exceptions.SMOSBlockDoubleRelease: if block to which the deleted
        entry is mapped has already been freed.

        :param idx: index of entry to be deleted
        :param force_delete: whether to delete the entry when there are still pending readers
        :return: 0 if successful, -1 if index out of range, 1 if permission denied
        """
        try:
            # check delete permission
            delete_permission = (self.entry_config_list[idx].pending_readers == 0)
            if not delete_permission and not force_delete:
                return 1

            # delete entry config and free
            entry_config = self.entry_config_list.pop(idx)
            block_idx = entry_config.mapped_block_idx
            if block_idx in self.free_block_list.queue:
                raise object_store_exceptions.SMOSBlockDoubleRelease(f"Block {block_idx} has already been"
                                                                     f"freed in data track {self.track_name}.")
            else:
                self.free_block_list.put(block_idx)
                return 0

        except IndexError:
            return -1

    # pop and free
    def pop_entry_config(self, force_pop=False):
        """
        Pop an entry from current data track. Note that block to which the entry is mapped
        will not be freed in this function (since data in the entry will be used after pop).
        Call free_block_mapping to free the block when data in the entry is no longer useful.


        :param force_pop: whether to pop the entry when there are still pending readers
        :return: [0, entry_config] if successful, [-1, None] if data track empty, [1, None]
                 if permission denied
        """
        try:
            # check permission
            pop_permission = (self.entry_config_list[0].pending_readers == 0)
            if not pop_permission and not force_pop:
                return 1, None

            # pop entry config
            entry_config = self.entry_config_list.pop(0)
            return 0, entry_config

        except IndexError:
            return -1, None

    def free_block_mapping(self, entry_config: util.EntryConfig):
        """
        Free a block associated with a previously popped entry.

        :exception object_store_exceptions.SMOSBlockDoubleRelease: if the block associated with
                   input entry_config has already been freed

        :param entry_config: a previously popped entry
        :return: always 0
        """
        # check if input entry_config is associated with current track
        if not entry_config.track_name == self.track_name:
            raise object_store_exceptions.SMOSTrackMismatch(f"Current track is {self.track_name}, while "
                                                            f"input entry_config is associated with track"
                                                            f" {entry_config.track_name}.")

        # free block mapping
        block_idx = entry_config.mapped_block_idx
        if block_idx in self.free_block_list.queue:
            raise object_store_exceptions.SMOSBlockDoubleRelease(f"Block {block_idx} has already been"
                                                                 f"freed in data track {self.track_name}.")
        else:
            self.free_block_list.put(block_idx)
            return 0
