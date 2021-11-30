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

    def __init__(self, obj_name, max_capacity, track_count, block_size_list):
        # save parameters
        self.obj_name = obj_name
        self.max_capacity = max_capacity
        self.track_count = track_count
        self.block_size_list = block_size_list

        # prepare tracks
        self.track_list = []
        for track_idx in range(track_count):
            track = get_data_track(track_name=f"{obj_name}:{track_idx}", shm_name=f"{obj_name}:{track_idx}",
                                   block_size=self.block_size_list[track_idx], max_capacity=self.max_capacity)
            self.track_list.append(track)

        # prepare lock
        self.lock = utils.RWLock()
