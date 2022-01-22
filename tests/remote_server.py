import time
import numpy as np
import SMOS
import SMOS_utils


def main():
    # server
    # connection = SMOS_utils.ConnectionDescriptor(ip="localhost", port=12365, authkey=b'antony')
    server = SMOS.Server()
    server_address = server.address()
    server.start()
    print("Server started.")

    # client
    # connection = SMOS_utils.ConnectionDescriptor(ip="10.200.13.18", port=10002, authkey=b'antony')
    client = SMOS.Client(server_address)
    print("Client started.")
    client.create_object(name="obj1", max_capacity=4, track_count=1, block_size=128)
    data1 = np.array([[b'1', b'1'], [b'1', b'1']])
    print(client.push_to_object(name="obj1", data=[data1]))
    print("Object put.")
    while True:
        print(server_address.ip, server_address.port)
        time.sleep(10)


if __name__ == '__main__':
    main()
