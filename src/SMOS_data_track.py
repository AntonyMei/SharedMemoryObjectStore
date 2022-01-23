"""
2021.11.25 Yixuan Mei
This file contains definition of class DataTrack for Shared Memory Object Store
"""

import queue
import random
from multiprocessing import shared_memory

import SMOS_exceptions
import SMOS_utils as utils
from SMOS_constants import SMOS_FAIL, SMOS_SUCCESS, SMOS_PERMISSION_DENIED, SMOS_MAX


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
        self.entry_config_list = {}
        self.next_key = 0
        self.free_block_list = queue.Queue(maxsize=max_capacity)
        for i in range(max_capacity):
            self.free_block_list.put(i)

    # write
    def allocate_block(self, entry_config: utils.EntryConfig):
        """
        Allocate a free block for a new entry and write into entry config.

        :param entry_config: configuration of new entry
        :return: SMOS_SUCCESS if successful,
                 SMOS_FAIL if no free block available
        """
        try:
            block_idx = self.free_block_list.get(block=False)
            entry_config.mapped_block_idx = block_idx
            entry_config.track_name = self.track_name
            return SMOS_SUCCESS
        except queue.Empty:
            return SMOS_FAIL

    def append_entry_config(self, entry_config: utils.EntryConfig):
        """
        Append configuration of new entry to this data track's configuration list

        :exception object_store_exceptions.SMOSEntryUnallocated: if entry_config is mapped to -1 block
        :exception object_store_exceptions.SMOSTrackMismatch: if track_name of current track is different
                   from track name of input entry_config

        :param entry_config: configuration of new entry
        :return: always [SMOS_SUCCESS, index of appended entry config]
        """
        # check if entry_config has been correctly allocated
        if entry_config.mapped_block_idx == -1:
            raise SMOS_exceptions.SMOSEntryUnallocated(f"Entry unallocated.")
        if not entry_config.track_name == self.track_name:
            raise SMOS_exceptions.SMOSTrackMismatch(f"Current track is {self.track_name}, while "
                                                    f"input entry_config is associated with track"
                                                    f" {entry_config.track_name}.")

        # append entry config
        self.entry_config_list[self.next_key] = entry_config
        self.next_key += 1
        return SMOS_SUCCESS, self.next_key - 1

    # read
    def read_entry_config(self, idx):
        """
        Read entry config at given index and add read reference to that entry.

        :param idx: index of entry to be read
        :return: [SMOS_SUCCESS, entry_config] if successful,
                 [SMOS_FAIL, None] if target entry does not exist
        """
        try:
            self.entry_config_list[idx].pending_readers += 1
            entry_config = self.entry_config_list[idx]
            return SMOS_SUCCESS, entry_config
        except KeyError:
            return SMOS_FAIL, None

    def read_latest_entry_config(self):
        """
        Read entry config of latest entry and add read reference to that entry.

        :return: [SMOS_SUCCESS, entry_idx, entry_config] if successful,
                 [SMOS_FAIL, None, None] if current track is empty
        """
        try:
            # get latest idx
            idx = max(list(self.entry_config_list.keys()))

            # get entry config
            self.entry_config_list[idx].pending_readers += 1
            entry_config = self.entry_config_list[idx]
            return SMOS_SUCCESS, idx, entry_config

        except ValueError:
            return SMOS_FAIL, None, None

    def release_read_reference(self, idx):
        """
        Release read reference on given entry.

        :exception object_store_exceptions.SMOSReadRefDoubleRelease: if a read reference is
                   released multiple times.

        :param idx: index of entry to be released
        :return: SMOS_SUCCESS if successful,
                 SMOS_FAIL if target entry does not exist
        """
        try:
            self.entry_config_list[idx].pending_readers -= 1
            if self.entry_config_list[idx].pending_readers < 0:
                raise SMOS_exceptions.SMOSReadRefDoubleRelease(f"Double release on track {self.track_name}"
                                                               f"index {idx}")
            return SMOS_SUCCESS
        except KeyError:
            return SMOS_FAIL

    # delete
    def delete_entry_config(self, idx, force_delete=False):
        """
        Delete an entry from current data track. Note that this is lazy delete, the actual
        data in shared memory is not erased.

        :exception object_store_exceptions.SMOSBlockDoubleRelease: if block to which the deleted
        entry is mapped has already been freed.

        :param idx: index of entry to be deleted
        :param force_delete: whether to delete the entry when there are still pending readers
        :return: SMOS_SUCCESS if successful,
                 SMOS_FAIL if target entry does not exist,
                 SMOS_PERMISSION_DENIED if permission denied
        """
        try:
            # check delete permission
            delete_permission = (self.entry_config_list[idx].pending_readers == 0)
            if not delete_permission and not force_delete:
                return SMOS_PERMISSION_DENIED

            # delete entry config and free
            entry_config = self.entry_config_list[idx]
            block_idx = entry_config.mapped_block_idx
            del self.entry_config_list[idx]
            if block_idx in self.free_block_list.queue:
                raise SMOS_exceptions.SMOSBlockDoubleRelease(f"Block {block_idx} has already been"
                                                             f"freed in data track {self.track_name}.")
            else:
                self.free_block_list.put(block_idx)
                return SMOS_SUCCESS

        except KeyError:
            return SMOS_FAIL

    # pop and free
    def pop_entry_config(self, force_pop=False):
        """
        Pop an entry from current data track. Note that block to which the entry is mapped
        will not be freed in this function (since data in the entry will be used after pop).
        Call free_block_mapping to free the block when data in the entry is no longer useful.

        :param force_pop: whether to pop the entry when there are still pending readers
        :return: [SMOS_SUCCESS, entry_config] if successful,
                 [SMOS_FAIL, None] if data track empty,
                 [SMOS_PERMISSION_DENIED, None] if permission denied
        """
        try:
            # get smallest item
            entry_idx = min(list(self.entry_config_list.keys()))

            # check permission
            pop_permission = (self.entry_config_list[entry_idx].pending_readers == 0)
            if not pop_permission and not force_pop:
                return SMOS_PERMISSION_DENIED, None

            # pop entry config
            entry_config = self.entry_config_list[entry_idx]
            del self.entry_config_list[entry_idx]
            return SMOS_SUCCESS, entry_config

        except ValueError:
            return SMOS_FAIL, None

    def free_block_mapping(self, entry_config: utils.EntryConfig):
        """
        Free a block associated with a previously popped entry.

        :exception object_store_exceptions.SMOSBlockDoubleRelease: if the block associated with
                   input entry_config has already been freed
        :exception object_store_exceptions.SMOSTrackMismatch: if track_name of current track is different
                   from track name of input entry_config

        :param entry_config: a previously popped entry
        :return: always SMOS_SUCCESS
        """
        # check if input entry_config is associated with current track
        if not entry_config.track_name == self.track_name:
            raise SMOS_exceptions.SMOSTrackMismatch(f"Current track is {self.track_name}, while "
                                                    f"input entry_config is associated with track"
                                                    f" {entry_config.track_name}.")

        # free block mapping
        block_idx = entry_config.mapped_block_idx
        if block_idx in self.free_block_list.queue:
            raise SMOS_exceptions.SMOSBlockDoubleRelease(f"Block {block_idx} has already been"
                                                         f"freed in data track {self.track_name}.")
        else:
            self.free_block_list.put(block_idx)
            return SMOS_SUCCESS

    # stop track for clean up
    def stop(self):
        """
        Unlinks current track's shared memory. This function is supposed to
        be called when (and only when) cleaning up for exit.

        :return: always SMOS_SUCCESS
        """
        self.shm.close()
        self.shm.unlink()
        return SMOS_SUCCESS

    # utility functions for DataTrack
    def get_entry_count(self):
        """
        Get the number of entries in current track.

        :return: entry count
        """
        return len(self.entry_config_list)

    def get_entry_offset(self, entry_config: utils.EntryConfig):
        """
        Get offset of given entry in shared memory space.

        :exception object_store_exceptions.SMOSTrackMismatch: if track_name of current track is different
                   from track name of input entry_config
        :exception SMOS_exceptions.SMOSMappingError: if mapped_block_idx in query is not smaller than
                   max_capacity

        :param entry_config: entry to be queried
        :return: always [SMOS_SUCCESS, offset]
        """
        # check if entry config is associated with current track
        if not entry_config.track_name == self.track_name:
            raise SMOS_exceptions.SMOSTrackMismatch(f"Current track is {self.track_name}, while "
                                                    f"input entry_config is associated with track"
                                                    f" {entry_config.track_name}.")

        # calculate result
        if entry_config.mapped_block_idx >= self.max_capacity:
            raise SMOS_exceptions.SMOSMappingError(f"Entry mapped to {entry_config.mapped_block_idx},"
                                                   f"which is out of range ([0, {self.max_capacity - 1}]).")
        else:
            return SMOS_SUCCESS, entry_config.mapped_block_idx * self.block_size


def get_data_track(track_name, shm_name, block_size, max_capacity):
    """
    Returns a data track. This function adds a random tail to shared memory name so that
    there two objects with same name can exist simultaneously in SMOS.

    :param track_name: name of new track
    :param shm_name: name of shared memory space used in new track
    :param block_size: size of each block in new track
    :param max_capacity: max capacity of new track
    :return: a new DataTrack object
    """

    while True:
        try:
            track = DataTrack(track_name=track_name, shm_name=f"{shm_name}_{random.randint(0, SMOS_MAX)}",
                              block_size=block_size, max_capacity=max_capacity)
            return track
        except FileExistsError:
            pass
