"""
2021.11.29 Yixuan Mei
This file contains class SharedMemoryObject, which is basic component of SMOS.
"""
import random
import sys

import SMOS_exceptions
import SMOS_utils as utils
from SMOS_data_track import get_data_track
from SMOS_constants import SMOS_FAIL, SMOS_SUCCESS, SMOS_PERMISSION_DENIED


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
            track = get_data_track(track_name=f"{name}:{self.track_name_list[track_idx]}",
                                   shm_name=f"{name}:{self.track_name_list[track_idx]}",
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
            status = self.track_list[track_idx].allocate_block(entry_config_list[track_idx])
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

        :param entry_config_list: configurations of new entry, one for each track
        :return: always SMOS_SUCCESS
        """
        # check input shape
        if not len(entry_config_list) == self.track_count:
            raise SMOS_exceptions.SMOSDimensionMismatch(f"There are {self.track_count} tracks, but only"
                                                        f"{len(entry_config_list)} items in input.")

        # append entry config
        self.lock.writer_enter()
        for track_idx in range(self.track_count):
            self.track_list[track_idx].append_entry_config(entry_config_list[track_idx])
        self.lock.writer_leave()

        # return
        return SMOS_SUCCESS
