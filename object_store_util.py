"""
2021.11.24 Yixuan Mei
This file contains utility functions for Shared Memory Object Store
"""

import os
import multiprocessing
import socket

import object_store_exceptions


class RWLock:

    def __init__(self):
        """
        This class implements a fair RW lock between multiple readers and writers.
        """
        self.reader_counter_lock = multiprocessing.Lock()
        self.writer_lock = multiprocessing.Lock()
        self.readwrite_lock = multiprocessing.Lock()
        self.reader_count = 0

    def reader_enter(self):
        self.writer_lock.acquire()
        self.reader_counter_lock.acquire()
        if self.reader_count == 0:
            self.readwrite_lock.acquire()
        self.reader_count += 1
        self.reader_counter_lock.release()
        self.writer_lock.release()

    def reader_leave(self):
        self.reader_counter_lock.acquire()
        self.reader_count -= 1
        if self.reader_count == 0:
            self.readwrite_lock.release()
        self.reader_counter_lock.release()

    def writer_enter(self):
        self.writer_lock.acquire()
        self.readwrite_lock.acquire()

    def writer_leave(self):
        self.readwrite_lock.release()
        self.writer_lock.release()


class ConnectionDescriptor:

    def __init__(self, ip, port, authkey):
        """ Describes the location of a python multiprocessing manager

        :param ip: ip address, could be string 'localhost'
        :param port: 0 - 65535
        :param authkey: a bytes string

        """
        self.ip = ip
        self.port = port
        self.authkey = authkey

    def get_ip(self):
        return self.ip

    def get_port(self):
        return self.port

    def get_authkey(self):
        return self.authkey


def get_local_free_port(num, low, high):
    """
    Get $num free ports between $low and $high if possible.

    :exception: object_store_exceptions.SMOSPortBusy will be raised if not enough free ports are available

    :param num: number of free ports required
    :param low: lower bound of port idx
    :param high: upper bound of port idx
    :return: free port list
    """
    port_list = []
    for port in range(low, high):
        # check if port is in use
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            is_busy = s.connect_ex(('localhost', port)) == 0
        if not is_busy:
            port_list.append(port)
        if len(port_list) == num:
            return port_list
    raise object_store_exceptions.SMOSPortBusy(f"Out of free ports (required {num}, available {len(port_list)})")
