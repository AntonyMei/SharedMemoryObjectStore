"""
2021.12.04 Yixuan Mei
This file is used to test SMOS correctness in single process case.
"""

import SMOS
import numpy as np

import SMOS_server


def main():
    # start server
    server = SMOS.Server()
    server_address = server.address()
    server.start()


if __name__ == '__main__':
    main()
