"""
2021.12.01 Yixuan Mei
This file contains class Client, which should be instantiated in every process that uses SMOS.
"""

import multiprocessing as mp
import time
from multiprocessing.managers import BaseManager

import SMOS_exceptions
import SMOS_utils as utils
from SMOS_constants import SMOS_SUCCESS
from SMOS_shared_memory_object_store import SharedMemoryObjectStore


class Client:

    def __init__(self, connection: utils.ConnectionDescriptor):
        pass
