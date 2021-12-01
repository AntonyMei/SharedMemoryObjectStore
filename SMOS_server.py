"""
2021.12.01 Yixuan Mei
This file contains class Server, which manages SharedMemoryObjectStore in a remote process.
"""
import time

import SMOS_exceptions
import SMOS_utils as utils
from SMOS_constants import SMOS_FAIL, SMOS_SUCCESS
from SMOS_shared_memory_object_store import SharedMemoryObjectStore
from multiprocessing.managers import BaseManager
import multiprocessing as mp


class Server:

    def __init__(self, connection: utils.ConnectionDescriptor = None):
        """
        SMOSServer runs an instance of SharedMemoryObjectStore remotely.

        :param connection: a ConnectionDescriptor that specifies address of SMOS server
        """
        if connection is not None:
            self.connection = connection
        else:
            port = utils.get_local_free_port(1, 5000, 5050)[0]
            self.connection = utils.ConnectionDescriptor(ip="localhost", port=port,
                                                         authkey="antony")
        self.server_process = None
        self.object_store = None

    def address(self):
        """
        Get the address that server listens

        :return: address that server listens
        """
        return self.connection

    def start(self):
        """
        Start server in another process.

        :return: always SMOS_SUCCESS
        """
        self.server_process = mp.Process(target=start_server, args=(self.connection, ))
        self.server_process.start()
        self.object_store = get_object_store(self.connection)
        return SMOS_SUCCESS

    def stop(self):
        """
        Stops server for safe exit.

        :return: always SMOS_SUCCESS
        """
        self.object_store.stop()
        self.server_process.terminate()
        return SMOS_SUCCESS


class SMOSManager(BaseManager):
    """
    Customized multiprocessing manager for SMOS.
    """
    pass


def start_server(connection: utils.ConnectionDescriptor):
    """
    Registers SMOS to python multiprocessing manager and start to serve.
    Note that this function must be called as a separate process.

    :exception SMOS_exceptions.SMOSServerDropOut: if server drops out by accident

    :param connection: a ConnectionDescriptor that specifies the address that SMOS server listens
    """
    # register SMOS
    object_store = SharedMemoryObjectStore()
    SMOSManager.register("get_object_store", callable=lambda: object_store)
    utils.log2terminal(info_type="Info", msg="Server registered.")

    # run server
    manager = SMOSManager(address=(connection.ip, connection.port), authkey=connection.authkey)
    server = manager.get_server()
    utils.log2terminal(info_type="Info", msg="Server started.")
    server.serve_forever()

    # we can never reach here
    raise SMOS_exceptions.SMOSServerDropOut("Server drops")


def get_object_store(connection: utils.ConnectionDescriptor):
    """
    Get a reference of SMOS that lives in server process.

    :param connection: a ConnectionDescriptor that specifies the address that SMOS server listens
    :return: a reference of SMOS
    """
    # register SMOS
    SMOSManager.register("get_object_store")
    manager = SMOSManager(address=(connection.ip, connection.port), authkey=connection.authkey)

    # connect to server
    connected = False
    while not connected:
        try:
            manager.connect()
            connected = True
        except ConnectionRefusedError:
            utils.log2terminal(info_type="Info", msg="Waiting for server to start...")
            time.sleep(1)

    # return reference
    return manager.get_object_store()
