import socket
import time
import numpy as np
from .Message import Message
from .Parameter import Parameter
from .LibraryHandle import LibraryHandle
from .MatrixHandle import MatrixHandle
from .WorkerClient import WorkerInfo


class DriverClient:

    id = 0
    hostname = ""
    address = ""
    port = 0

    client_id = 0
    session_id = 0
    library_id = 0

    max_alchemist_workers = 0

    sock = []

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

    def start_message(self, command):
        return self.output_message.start(self.client_id, self.session_id, command)

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
        self.start_message("SEND_TEST_STRING")
        self.output_message.write_string(test_message)
        self.send_message()
        self.receive_message()
        print("Alchemist returned: '" + self.input_message.read_string() + "'")

    def request_test_string(self):
        print("Requesting test message from Alchemist.")
        self.start_message("REQUEST_TEST_STRING")
        self.send_message()
        self.receive_message()
        print("Alchemist returned: '" + self.input_message.read_string() + "'")

    def load_library(self, name, path=""):

        self.start_message("LOAD_LIBRARY")
        self.output_message.write_string(name)
        self.output_message.write_string(path)
        self.send_message()
        self.receive_message()

        self.input_message.print()
        lh = LibraryHandle(self.input_message.read_short(), name, path)

        return lh

    def run_task(self, lib_id, task_name, in_args):
        self.start_message("RUN_TASK")
        self.output_message.write_library_id(lib_id)
        self.output_message.write_string(task_name)
        self.serialize_parameters(in_args)
        self.send_message()
        self.receive_message()

        return self.deserialize_parameters()

    def serialize_parameters(self, in_args):

        for key, value in in_args.items():
            self.output_message.write_string(value.name)
            if value.datatype == "BYTE":
                self.output_message.write_byte(value.value)
            elif value.datatype == "SHORT":
                self.output_message.write_short(value.value)
            elif value.datatype == "INT":
                self.output_message.write_int(value.value)
            elif value.datatype == "LONG":
                self.output_message.write_long(value.value)
            elif value.datatype == "FLOAT":
                self.output_message.write_float(value.value)
            elif value.datatype == "DOUBLE":
                self.output_message.write_double(value.value)
            elif value.datatype == "CHAR":
                self.output_message.write_char(value.value)
            elif value.datatype == "STRING":
                self.output_message.write_string(value.value)
            elif value.datatype == "LIBRARY_ID":
                self.output_message.write_library_id(value.value)
            elif value.datatype == "MATRIX_ID":
                self.output_message.write_matrix_id(value.value)

    def deserialize_parameters(self):

        out_args = {}

        while not self.input_message.eom():
            parameter_name = self.input_message.read_string()

            parameter_datatype = self.input_message.preview_next_datatype()

            if parameter_datatype == "BYTE":
                out_args[parameter_name] = Parameter(parameter_name, parameter_datatype, self.input_message.read_byte())
            elif parameter_datatype == "SHORT":
                out_args[parameter_name] = Parameter(parameter_name, parameter_datatype, self.input_message.read_short())
            elif parameter_datatype == "INT":
                out_args[parameter_name] = Parameter(parameter_name, parameter_datatype, self.input_message.read_int())
            elif parameter_datatype == "LONG":
                out_args[parameter_name] = Parameter(parameter_name, parameter_datatype, self.input_message.read_long())
            elif parameter_datatype == "FLOAT":
                out_args[parameter_name] = Parameter(parameter_name, parameter_datatype, self.input_message.read_float())
            elif parameter_datatype == "DOUBLE":
                out_args[parameter_name] = Parameter(parameter_name, parameter_datatype, self.input_message.read_double())
            elif parameter_datatype == "CHAR":
                out_args[parameter_name] = Parameter(parameter_name, parameter_datatype, self.input_message.read_char())
            elif parameter_datatype == "STRING":
                out_args[parameter_name] = Parameter(parameter_name, parameter_datatype, self.input_message.read_string())
            elif parameter_datatype == "LIBRARY_ID":
                out_args[parameter_name] = Parameter(parameter_name, parameter_datatype, self.input_message.read_library_id())
            elif parameter_datatype == "MATRIX_ID":
                out_args[parameter_name] = Parameter(parameter_name, parameter_datatype, self.input_message.read_matrix_id())
            elif parameter_datatype == "MATRIX_INFO":
                out_args[parameter_name] = Parameter(parameter_name, parameter_datatype, self.input_message.read_matrix_info())

        return out_args

    def send_matrix_info(self, name="", num_rows=0, num_cols=0, sparse=False, layout=0):
        self.start_message("SEND_MATRIX_INFO")
        self.output_message.write_string(name)
        self.output_message.write_long(num_rows)            # Number of rows
        self.output_message.write_long(num_cols)            # Number of columns
        if sparse:
            self.output_message.write_byte(1)               # Type: sparse
        else:
            self.output_message.write_byte(0)               # Type: dense
        self.output_message.write_byte(0)                   # Layout: by rows (default)
        self.send_message()
        self.receive_message()
        self.input_message.print()
        return self.input_message.read_matrix_info()

    def extract_layout(self, num_rows):
        layout = np.zeros(num_rows, dtype=np.int16)

        for i in range(0, num_rows):
            layout[i] = self.input_message.read_short()

        return layout

    def request_workers(self, num_requested_workers):
        if num_requested_workers == 1:
            print("Requesting 1 worker from Alchemist")
        else:
            print("Requesting {} workers from Alchemist".format(num_requested_workers))

        self.start_message("REQUEST_WORKERS")
        self.output_message.write_short(num_requested_workers)
        self.send_message()
        self.receive_message()
        num_allocated_workers = self.input_message.read_short()
        print("Total allocated {} workers:".format(num_allocated_workers))
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
        self.start_message("LIST_AVAILABLE_LIBRARIES")
        return self.send_message

    # def load_library(self, lib_name):
    #     print("Loading " + lib_name + " ... success")
    #     return LibraryHandle(lib_name)

    # def load_from_hdf5(self, file_name, dataset_name):
    #     print("Loading '" + dataset_name + "' from '" + file_name + "' ... success")
    #     A = MatrixHandle()
    #     A.set(24, 'dense', 1200000, 10000, 2)
    #     return A

    def yield_workers(self, yielded_workers=[]):
        self.start_message("YIELD_WORKERS")
        for i in yielded_workers:
            self.output_message.write_short(i)
        self.send_message()
        self.receive_message()

        deallocated_workers = []
        while not self.input_message.eom():
            deallocated_workers.append(self.input_message.read_short())

        return deallocated_workers

    def get_matrix_info(self):
        self.start_message("REQUEST_MATRIX_INFO")
        return self.send_message

    def list_all_workers(self, preamble):
        self.start_message("LIST_ALL_WORKERS")
        self.output_message.write_string(preamble)
        self.send_message()
        self.receive_message()

        return self.input_message.read_string()

    def list_active_workers(self, preamble):
        self.start_message("LIST_ACTIVE_WORKERS")
        self.output_message.write_string(preamble)
        self.send_message()
        self.receive_message()

        return self.input_message.read_string()

    def list_inactive_workers(self, preamble):
        self.start_message("LIST_INACTIVE_WORKERS")
        self.output_message.write_string(preamble)
        self.send_message()
        self.receive_message()

        return self.input_message.read_string()

    def list_assigned_workers(self, preamble):
        self.start_message("LIST_ASSIGNED_WORKERS")
        self.output_message.write_string(preamble)
        self.send_message()
        self.receive_message()

        return self.input_message.read_string()

    def retry_connection(self, retries=3):
        while retries > 0:
            print("Reconnecting to Alchemist (try {}/{})".format(3 - retries + 1, 3))
            time.sleep(3)
            if self.connect():
                return True
            else:
                self.retry_connection(retries-1)

        return False

    def reset_socket(self):
        self.close_socket()
        return self.retry_connection()

    def close_socket(self):
        print("Closing socket")
        self.sock.close()

    def close(self):
        self.close_socket()
