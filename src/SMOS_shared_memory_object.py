"""
2021.11.29 Yixuan Mei
This file contains class SharedMemoryObject, which is basic component of SMOS.
"""

import SMOS_exceptions
import SMOS_utils as utils
from SMOS_constants import SMOS_FAIL, SMOS_SUCCESS
from SMOS_data_track import get_data_track


class SharedMemoryObject:

    def __init__(self, name, max_capacity, track_count, block_size_list, track_name_list=None):
        """
        SharedMemoryObject is a basic component of SMOS. Each object stored in SMOS is represented
        by a SharedMemoryObject. Such objects can have multiple DataTracks for its data components.
        In order to avoid concurrency issues, shared memory object uses RWLock to synchronize accesses
        from different processes.

        :exception SMOS_exceptions.SMOSDimensionMismatch: if length of block_size_list does not
                   equal to track_count

        :param name: name of this SharedMemoryObject
        :param max_capacity: maximum number of objects that can be stored in this SharedMemoryObject
        :param track_count: number of tracks in this SharedMemoryObject
        :param block_size_list: block size of each track
        :param track_name_list: (optional) name of each track
        """
        # save parameters
        self.name = name
        self.track_count = track_count
        self.max_capacity = max_capacity

        # save block sizes
        if not len(block_size_list) == track_count:
            raise SMOS_exceptions.SMOSDimensionMismatch(f"There are {track_count} tracks, but only"
                                                        f"{len(block_size_list)} items in block size list.")
        self.block_size_list = block_size_list

        # save track names
        if track_name_list is not None and len(track_name_list) == track_count:
            self.track_name_list = track_name_list
        else:
            self.track_name_list = list(range(track_count))

        # prepare tracks
        self.track_list = []
        for track_idx in range(track_count):
            track = get_data_track(track_name=f"{name}_{self.track_name_list[track_idx]}",
                                   shm_name=f"{name}_{self.track_name_list[track_idx]}",
                                   block_size=self.block_size_list[track_idx], max_capacity=self.max_capacity)
            self.track_list.append(track)

        # save shared memory names
        self.shm_name_list = [track.shm_name for track in self.track_list]

        # prepare lock
        self.lock = utils.RWLock()

    # write
    def allocate_block(self, entry_config_list: [utils.EntryConfig]):
        """
        Allocate a free block for a new entry and write into entry config.

        :exception SMOS_exceptions.SMOSDimensionMismatch: if length of input is
                   different of number of tracks
        :exception SMOS_exceptions.SMOSTrackUnaligned: if some tracks are able to
                   allocate a new block while others are not

        :param entry_config_list: configurations of new entry, one for each track
        :return: SMOS_SUCCESS if successful,
                 SMOS_FAIL if no free block available
        """
        # check input shape
        if not len(entry_config_list) == self.track_count:
            raise SMOS_exceptions.SMOSDimensionMismatch(f"There are {self.track_count} tracks, but only"
                                                        f"{len(entry_config_list)} items in input.")

        # allocate block
        status_list = []
        for track_idx in range(self.track_count):
            status = self.track_list[track_idx].allocate_block(entry_config=entry_config_list[track_idx])
            status_list.append(status)

        # return
        if not len(set(status_list)) == 1:
            raise SMOS_exceptions.SMOSTrackUnaligned("Track unaligned.")
        return status_list[0]

    def append_entry_config(self, entry_config_list: [utils.EntryConfig]):
        """
        Append configurations of new entry into SharedMemoryObject's tracks, one
        configuration for each track.

        :exception SMOS_exceptions.SMOSDimensionMismatch: if length of input is
                   different of number of tracks
        :exception SMOS_exceptions.SMOSTrackUnaligned: if different tracks return
                   different index for newly appended entry

        :param entry_config_list: configurations of new entry, one for each track
        :return: always [SMOS_SUCCESS, index of appended entry config]
        """
        # check input shape
        if not len(entry_config_list) == self.track_count:
            raise SMOS_exceptions.SMOSDimensionMismatch(f"There are {self.track_count} tracks, but only"
                                                        f"{len(entry_config_list)} items in input.")

        # append entry config
        self.lock.writer_enter()
        entry_idx_list = []
        for track_idx in range(self.track_count):
            track = self.track_list[track_idx]
            _, entry_idx = track.append_entry_config(entry_config=entry_config_list[track_idx])
            entry_idx_list.append(entry_idx)
        self.lock.writer_leave()

        # check data integrity
        if not len(set(entry_idx_list)) == 1:
            raise SMOS_exceptions.SMOSTrackUnaligned("Track unaligned.")

        # return
        return SMOS_SUCCESS, entry_idx_list[0]

    # read
    def read_entry_config(self, idx):
        """
        Read entry config at given index and add read reference to that entry.

        :exception SMOS_exceptions.SMOSTrackUnaligned: if some tracks return
                   index out of range while others do not

        :param idx: index of entry to be read
        :return: [SMOS_SUCCESS, entry_config_list] if successful,
                 [SMOS_FAIL, None] if target entry does not exist
        """
        # read entry config
        status_list = []
        entry_config_list = []
        self.lock.reader_enter()
        for track in self.track_list:
            status, entry_config = track.read_entry_config(idx=idx)
            status_list.append(status)
            entry_config_list.append(entry_config)
        self.lock.reader_leave()

        # check data integrity
        if not len(set(status_list)) == 1:
            raise SMOS_exceptions.SMOSTrackUnaligned("Track unaligned.")

        # return
        if status_list[0] == SMOS_SUCCESS:
            return SMOS_SUCCESS, entry_config_list
        else:
            return SMOS_FAIL, None

    def release_read_reference(self, idx):
        """
        Release read reference on given entry

        :exception SMOS_exceptions.SMOSTrackUnaligned: if some tracks return
                   index out of range while others do not

        :param idx: index of entry to be released
        :return: SMOS_SUCCESS if successful,
                 SMOS_FAIL if target entry does not exist
        """
        # release read reference
        status_list = []
        self.lock.reader_enter()
        for track in self.track_list:
            status = track.release_read_reference(idx=idx)
            status_list.append(status)
        self.lock.reader_leave()

        # check integrity and return
        if not len(set(status_list)) == 1:
            raise SMOS_exceptions.SMOSTrackUnaligned("Track unaligned.")
        return status_list[0]

    # delete
    def delete_entry_config(self, idx, force_delete=False):
        """
        Delete entry at idx from current SharedMemoryObject. Note that this is lazy
        delete, the actual data in shared memory is not erased.

        :exception SMOS_exceptions.SMOSTrackUnaligned: if different tracks have different
                   return value to delete operation

        :param idx: index of entry to be deleted
        :param force_delete: whether to delete the entry when there are still pending readers
        :return: SMOS_SUCCESS if successful,
                 SMOS_FAIL if target entry does not exist,
                 SMOS_PERMISSION_DENIED if permission denied
        """
        # delete entry config
        status_list = []
        self.lock.writer_enter()
        for track in self.track_list:
            status = track.delete_entry_config(idx=idx, force_delete=force_delete)
            status_list.append(status)
        self.lock.writer_leave()

        # check integrity and return
        if not len(set(status_list)) == 1:
            raise SMOS_exceptions.SMOSTrackUnaligned("Track unaligned.")
        return status_list[0]

    # pop and free
    def pop_entry_config(self, force_pop=False):
        """
        Pop an entry from SharedMemoryObject. Note that blocks to which the entry is mapped
        will not be freed in this function (since data in the entry will be used after pop).
        Call free_block_mapping to free the block when data in the entry is no longer useful.

        :exception SMOS_exceptions.SMOSTrackUnaligned: if different tracks have different
                   return value to delete operation

        :param force_pop: whether to pop the entry when there are still pending readers
        :return: [SMOS_SUCCESS, entry_config_list] if successful,
                 [SMOS_FAIL, None] if data track empty,
                 [SMOS_PERMISSION_DENIED, None] if permission denied
        """
        # pop entry config
        status_list = []
        entry_config_list = []
        self.lock.writer_enter()
        for track in self.track_list:
            status, entry_config = track.pop_entry_config(force_pop=force_pop)
            status_list.append(status)
            entry_config_list.append(entry_config)
        self.lock.writer_leave()

        # check integrity
        if not len(set(status_list)) == 1:
            raise SMOS_exceptions.SMOSTrackUnaligned("Track unaligned.")

        # return
        if status_list[0] == SMOS_SUCCESS:
            return SMOS_SUCCESS, entry_config_list
        else:
            return status_list[0], None

    def free_block_mapping(self, entry_config_list: [utils.EntryConfig]):
        """
        Free blocks associated with a previously popped entry.

        :param entry_config_list: configurations of entry to be freed
        :return: always SMOS_SUCCESS
        """
        # check input shape
        if not len(entry_config_list) == self.track_count:
            raise SMOS_exceptions.SMOSDimensionMismatch(f"There are {self.track_count} tracks, but only"
                                                        f"{len(entry_config_list)} items in input.")

        # free block mapping and return
        for track_idx in range(self.track_count):
            self.track_list[track_idx].free_block_mapping(entry_config=entry_config_list[track_idx])
        return SMOS_SUCCESS

    # stop SharedMemoryObject
    def stop(self):
        """
        Stop SharedMemoryObject for safe exit.

        :return: always SMOS_SUCCESS
        """
        # stop tracks in this object
        for track in self.track_list:
            track.stop()

        # release lock
        del self.lock

    # utility functions for SharedMemoryObject
    def get_shm_name_list(self):
        """
        Get name of shared memories used in this object.

        :return: list of shared memory names
        """
        return self.shm_name_list

    def get_entry_offset(self, entry_config_list: [utils.EntryConfig]):
        """
        Get offset of each track for given entry in shared memory space.

        :exception SMOS_exceptions.SMOSDimensionMismatch: if length of input is
                   different of number of tracks

        :param entry_config_list: configurations of entry to be queried
        :return: always [SMOS_SUCCESS, offset_list]
        """
        # check input shape
        if not len(entry_config_list) == self.track_count:
            raise SMOS_exceptions.SMOSDimensionMismatch(f"There are {self.track_count} tracks, but only"
                                                        f"{len(entry_config_list)} items in input.")

        # get entry offset
        offset_list = []
        for track_idx in range(self.track_count):
            _, offset = self.track_list[track_idx].get_entry_offset(entry_config=entry_config_list[track_idx])
            offset_list.append(offset)

        # return
        return SMOS_SUCCESS, offset_list

    def get_entry_idx_list(self):
        """
        Get index of all entries in current SharedMemoryObject.

        :return: entry_idx_list
        """
        # get entry_idx_list
        self.lock.reader_enter()
        entry_idx_list = self.track_list[0].entry_config_list.keys()
        self.lock.reader_leave()

        # return
        return entry_idx_list

    def get_entry_count(self):
        """
        Get number of entries in current SharedMemoryObject. Note that this
        function also checks data integrity (i.e. whether all tracks are
        properly aligned).

        :exception SMOS_exceptions.SMOSTrackUnaligned: if tracks have different
                   number of entries in them

        :return: entry count
        """
        # get entry count
        entry_count_list = []
        self.lock.reader_enter()
        for track in self.track_list:
            entry_count = track.get_entry_count()
            entry_count_list.append(entry_count)
        self.lock.reader_leave()

        # check data integrity and return
        if not len(set(entry_count_list)) == 1:
            raise SMOS_exceptions.SMOSTrackUnaligned("Track unaligned.")
        return entry_count_list[0]
