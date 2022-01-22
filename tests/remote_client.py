import time

import SMOS
import SMOS_utils


def main():
    connection = SMOS_utils.ConnectionDescriptor(ip="10.200.13.18", port=5000, authkey=b'antony')
    print(connection.ip, connection.port)
    client = SMOS.Client(connection)
    print("client connected")
    while True:
        status, handle_batch, data_batch = client.batch_read_from_object(name="obj1", entry_idx_batch=[0])
        if status == SMOS.SMOS_SUCCESS:
            print(data_batch)
            for handle, data in zip(handle_batch, data_batch):
                print(data)
                client.release_entry(object_handle=handle)
        else:
            print("read failed")
        print()
        time.sleep(2)


if __name__ == '__main__':
    main()
