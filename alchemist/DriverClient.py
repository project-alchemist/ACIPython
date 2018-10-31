import socket
import time
import numpy as np
from .Message import Message
from .LibraryHandle import TestLib
from .MatrixHandle import MatrixHandle
from .WorkerClient import WorkerInfo


class DriverClient:

    id = 0
    hostname = ""
    address = ""
    port = 0

    client_id = 0
    session_id = 0

    max_alchemist_workers = 3

    sock = []

    libraries = {}

    input_message = Message()
    output_message = Message()

    def __init__(self):
        self.id = 0
        self.hostname = "0.0.0.0"
        self.address = "0.0.0.0"
        self.port = 24960

        self.sock = []

    def __del__(self):
        print("Closing driver client")
        self.close()

    def connect(self):

        # Create a TCP/IP socket
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Connect the socket to the port where Alchemist is listening
        server_address = (self.address, self.port)
        print('Connecting to Alchemist at {0}:{1} ...'.format(*server_address))
        try:
            self.sock.connect(server_address)
        except socket.gaierror:
            print("ERROR: Address-related error connecting to Alchemist")
            return False
        except ConnectionRefusedError:
            print("ERROR: Alchemist appears to be offline")
            return False

        if self.handshake():
            print("Connected to Alchemist!")
            return True
        else:
            print("Unable to connect to Alchemist")
            return False

    def send_message(self):
        try:
            self.output_message.finish()
            # self.output_message.print()
            self.sock.sendall(self.output_message.get())
            self.output_message.reset()
            return True
        except InterruptedError:
            print("ERROR: Unable to send message (InterruptedError)")
            self.reset_socket()
        except ConnectionError:
            print("ERROR: Unable to send message (ConnectionError)")
            self.reset_socket()

    def receive_message(self):
        try:
            header = self.sock.recv(self.input_message.get_header_length())
            self.input_message.reset()
            self.input_message.add_header(header)
            remaining_body_length = self.input_message.get_body_length()
            while remaining_body_length > 0:
                packet = self.sock.recv(min(remaining_body_length, 8192))
                if not packet:
                    return False
                remaining_body_length -= len(packet)
                self.input_message.add_packet(packet)
            # self.input_message.print()
            return True
        except InterruptedError:
            print("ERROR: Unable to send message (InterruptedError)")
            return self.reset_socket
        except ConnectionError:
            print("ERROR: Unable to send message (ConnectionError)")
            return self.reset_socket

    def handshake(self):
        self.output_message.start(0, 0, "HANDSHAKE")
        self.output_message.write_byte(4)
        self.output_message.write_short(1234)
        self.output_message.write_string("ABCD")
        self.send_message()
        self.receive_message()
        if self.input_message.read_short() == 4321:
            if self.input_message.read_string() == "DCBA":
                self.client_id = self.input_message.read_client_id()
                self.session_id = self.input_message.read_session_id()
                return True
        return False

    def get_max_alchemist_workers(self):
        return self.max_alchemist_workers

    def send_test_string(self):
        test_message = "This is a test message from client {}".format(self.client_id)
        print("Sending test message: '" + test_message + "'")
        self.output_message.start(self.client_id, self.session_id, "SEND_TEST_STRING")
        self.output_message.write_string(test_message)
        self.send_message()
        self.receive_message()
        print("Alchemist returned: '" + self.input_message.read_string() + "'")

    def request_test_string(self):
        print("Requesting test message from Alchemist.")
        self.output_message.start(self.client_id, self.session_id, "REQUEST_TEST_STRING")
        self.send_message()
        self.receive_message()
        print("Alchemist returned: '" + self.input_message.read_string() + "'")

    def load_library(self, name):

        if name == "TestLib":
            path = "/Users/kai/Projects/AlLib/target/allib.dylib"
            self.libraries[name] = TestLib(name, path)

        # self.output_message.start(self.client_id, self.session_id, "LOAD_LIBRARY")
        # self.output_message.write_string(name)
        # self.output_message.write_string(path)
        # self.send_message()
        # self.receive_message()

        return self.libraries[name]

    def truncated_svd(self, lh, name, mh, rank):
        method = 0
        self.output_message.start(self.client_id, self.session_id, "RUN_TASK")
        self.output_message.write_string(name)
        self.output_message.write_short(mh.id)
        self.output_message.write_int(rank)
        self.output_message.write_byte(method)
        self.send_message()
        self.receive_message()

        matrix_id = self.input_message.read_short()
        num_rows = self.input_message.read_long()
        num_cols = self.input_message.read_long()
        row_layout = self.extract_layout(num_rows)

        U = MatrixHandle().set(matrix_id, 'dense', num_rows, num_cols, 1, row_layout)

        matrix_id = self.input_message.read_short()
        num_rows = self.input_message.read_long()
        num_cols = self.input_message.read_long()
        row_layout = self.extract_layout(num_rows)

        S = MatrixHandle().set(matrix_id, 'dense', num_rows, num_cols, 1, row_layout)

        matrix_id = self.input_message.read_short()
        num_rows = self.input_message.read_long()
        num_cols = self.input_message.read_long()
        row_layout = self.extract_layout(num_rows)

        V = MatrixHandle().set(matrix_id, 'dense', num_rows, num_cols, 1, row_layout)

        return U, S, V

    def send_matrix_info(self, num_rows, num_cols):
        print("Sending matrix info to Alchemist.")
        self.output_message.start(self.client_id, self.session_id, "MATRIX_INFO")
        self.output_message.write_byte(0)                   # Type: dense
        self.output_message.write_byte(0)                   # Layout: by rows (default)
        self.output_message.write_long(num_rows)            # Number of rows
        self.output_message.write_long(num_cols)            # Number of columns
        self.send_message()
        self.receive_message()

        matrix_id = self.input_message.read_short()
        num_rows = self.input_message.read_long()
        num_cols = self.input_message.read_long()
        row_layout = self.extract_layout(num_rows)

        return MatrixHandle().set(matrix_id, 'dense', num_rows, num_cols, 1, row_layout)

    def extract_layout(self, num_rows):
        layout = np.zeros(num_rows, dtype=np.int16)

        for i in range(0, num_rows):
            layout[i] = self.input_message.read_short()

        return layout

    def request_workers(self, num_requested_workers):
        print("Requesting {} workers from Alchemist".format(num_requested_workers))
        self.output_message.start(self.client_id, self.session_id, "REQUEST_WORKERS")
        self.output_message.write_short(num_requested_workers)
        self.send_message()
        self.receive_message()
        num_allocated_workers = self.input_message.read_short()
        print("Allocated {} workers:".format(num_allocated_workers))
        workers = []
        for i in range(0, num_allocated_workers):
            worker = WorkerInfo()
            worker.id = self.input_message.read_short()
            worker.hostname = self.input_message.read_string()
            worker.address = self.input_message.read_string()
            worker.port = self.input_message.read_short()
            workers.append(worker)
        return workers

    def list_available_libraries(self):
        return 0

    # def load_library(self, lib_name):
    #     print("Loading " + lib_name + " ... success")
    #     return LibraryHandle(lib_name)

    # def load_from_hdf5(self, file_name, dataset_name):
    #     print("Loading '" + dataset_name + "' from '" + file_name + "' ... success")
    #     A = MatrixHandle()
    #     A.set(24, 'dense', 1200000, 10000, 2)
    #     return A

    def yield_workers(self):
        self.output_message.start(self.client_id, self.session_id, "YIELD_WORKERS")
        return self.send_message

    def get_matrix_info(self):
        self.output_message.start(self.client_id, self.session_id, "MATRIX_INFO")
        return self.send_message

    def list_all_alchemist_workers(self):
        self.output_message.start(self.client_id, self.session_id, "LIST_ALL_WORKERS")
        self.send_message()
        self.receive_message()

        self.max_alchemist_workers = self.input_message.read_short()
        return self.input_message.read_string()

    def list_active_alchemist_workers(self):
        self.output_message.start(self.client_id, self.session_id, "LIST_ACTIVE_WORKERS")
        return self.send_message()

    def list_inactive_alchemist_workers(self):
        self.output_message.start(self.client_id, self.session_id, "LIST_INACTIVE_WORKERS")
        return self.send_message()

    def list_assigned_alchemist_workers(self):
        self.output_message.start(self.client_id, self.session_id, "LIST_ASSIGNED_WORKERS")
        return self.send_message()

    def retry_connection(self, retries=3):
        while retries > 0:
            print("Reconnecting to Alchemist (try {}/{})".format(3 - retries + 1, 3))
            time.sleep(3)
            if self.connect:
                return True
            else:
                self.retry_connection(retries-1)

        return False

    def reset_socket(self):
        self.close_socket()
        return self.connect()

    def close_socket(self):
        print("Closing socket")
        self.sock.close()

    def close(self):
        self.close_socket()
