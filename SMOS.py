"""
2021.12.04 Yixuan Mei
This file contains everything that needs to be imported for SMOS.
"""

import SMOS_exceptions
import SMOS_utils

from SMOS_constants import SMOS_SUCCESS, SMOS_FAIL, SMOS_PERMISSION_DENIED
from SMOS_utils import ConnectionDescriptor
from SMOS_server import Server
from SMOS_client import Client


# serialize and deserialize (one-copy)
def serialize(obj, buf):
    """
    Serialize target object into buffer. Note that this should only be
    used for non-numpy objects, since numpy arrays have more efficient
    zero-copy ser/des protocol.

    :param obj: object to be serialized
    :param buf: a memoryview object, e.g. the one returned by open_shm
           for non-numpy objects.
    """
    # check input
    if not type(buf) == memoryview:
        raise SMOS_exceptions.SMOSInputTypeError(f"memoryview expected for buf, get {type(buf)}")

    # serialize
    pickle.dump(obj=obj, file=buf.obj, protocol=pickle.HIGHEST_PROTOCOL)


def deserialize(buf):
    """
    Deserialize target object from buffer. Note that this should only be
    used for non-numpy objects, since numpy arrays have more efficient
    zero-copy ser/des protocol.

    :param buf: buffer that stores the data stream
    :return: deserialized_object
    """
    deserialized_object = pickle.loads(data=buf)
    return deserialized_object
