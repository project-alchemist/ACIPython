import socket
import time
import numpy as np
from .Message import Message
from .Parameter import Parameter
from .LibraryHandle import LibraryHandle
from .MatrixHandle import MatrixHandle
from .WorkerInfo import WorkerInfo


class Client:

    id = 0
    hostname = ""
    address = ""
    port = 0

    client_id = 0
    session_id = 0

    sock = []

    connected = False

    input_message = Message()
    output_message = Message()

    def __init__(self, id=0, hostname="host", address="0.0.0.0", port=24960):
        self.id = id
        self.hostname = hostname
        self.address = address
        self.port = port

        self.connected = False

        self.sock = []

    def connect(self):

        if ~self.connected:
            # Create a TCP/IP socket
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            # Connect the socket to the port where Alchemist is listening
            server_address = (self.address, self.port)
            print('Connecting to Alchemist at {0}:{1} ...'.format(*server_address), end="", flush=True)
            try:
                self.sock.connect(server_address)
            except socket.gaierror:
                self.connected = False
                print("ERROR: Address-related error connecting to Alchemist")
            except ConnectionRefusedError:
                self.connected = False
                print("ERROR: Alchemist appears to be offline")

            if self.handshake():
                self.connected = True
                print("Connected to Alchemist!")
            else:
                self.connected = False
                print("Unable to connect to Alchemist")

        return self.connected

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
        self.output_message.write_double(1.11)
        self.output_message.write_double(2.22)
        test_matrix = 1.11*np.arange(3, 15).reshape((4, 3))
        self.output_message.write_matrix_block(test_matrix, [0, 3, 1], [0, 2, 1])
        self.send_message()
        self.receive_message()

        return self.validate_handshake()

    def validate_handshake(self):

        if self.input_message.command_code == 1 and self.input_message.read_short() == 4321 and \
                self.input_message.read_string() == "DCBA" and self.input_message.read_double():
            self.client_id = self.input_message.read_client_id()
            self.session_id = self.input_message.read_session_id()
            return True
        return False

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


class DriverClient(Client):

    library_id = 0

    max_alchemist_workers = 0

    def __del__(self):
        print("Closing driver client")
        self.close()

    def get_max_alchemist_workers(self):
        return self.max_alchemist_workers

    def load_library(self, name, path=""):
        self.start_message("LOAD_LIBRARY")
        self.output_message.write_string(name)
        self.output_message.write_string(path)
        self.send_message()
        self.receive_message()
        return self.input_message.read_library_id()

    def run_task(self, lib_id, task_name, in_args):
        self.start_message("RUN_TASK")
        self.output_message.write_library_id(lib_id)
        self.output_message.write_string(task_name)
        for name, p in in_args.items():
            self.output_message.write_parameter(p)
        self.send_message()
        self.receive_message()

        out_args = {}

        while not self.input_message.eom():
            p = self.input_message.read_parameter()
            out_args[p.name] = p

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
        self.output_message.write_byte(layout)              # Layout
        self.send_message()
        self.receive_message()
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
            workers.append(self.input_message.read_worker_info())
        return workers

    def yield_workers(self, yielded_workers=[]):
        self.start_message("YIELD_WORKERS")
        self.output_message.write_short(len(yielded_workers))
        for i in yielded_workers:
            self.output_message.write_short(i)
        self.send_message()
        self.receive_message()
        num_deallocated_workers = self.input_message.read_short()
        workers = []
        for _ in range(num_deallocated_workers):
            workers.append(self.input_message.read_worker_info())
        return workers

    def list_all_workers(self):
        self.start_message("LIST_ALL_WORKERS")
        self.send_message()
        self.receive_message()
        num_workers = self.input_message.read_short()
        workers = []
        for _ in range(num_workers):
            workers.append(self.input_message.read_worker_info())
        return workers

    def list_active_workers(self):
        self.start_message("LIST_ACTIVE_WORKERS")
        self.send_message()
        self.receive_message()
        num_active_workers = self.input_message.read_short()
        workers = []
        for _ in range(num_active_workers):
            workers.append(self.input_message.read_worker_info())
        return workers

    def list_inactive_workers(self):
        self.start_message("LIST_INACTIVE_WORKERS")
        self.send_message()
        self.receive_message()
        num_inactive_workers = self.input_message.read_short()
        workers = []
        for _ in range(num_inactive_workers):
            workers.append(self.input_message.read_worker_info())
        return workers

    def list_assigned_workers(self):
        self.start_message("LIST_ASSIGNED_WORKERS")
        self.send_message()
        self.receive_message()
        num_assigned_workers = self.input_message.read_short()
        workers = []
        for _ in range(num_assigned_workers):
            workers.append(self.input_message.read_worker_info())
        return workers

    def get_matrix_info(self):
        self.start_message("REQUEST_MATRIX_INFO")
        return self.send_message

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


class WorkerClient(Client):

    def send_matrix_block(self, ah, matrix, rows=[0, 0, 1], cols=[0, 0, 1]):

        if rows[1] == 0:
            rows[1] = matrix.shape[0]
        row_range = np.array(np.arange(rows[0], rows[1], rows[2]), dtype=np.intp)
        rows[1] -= 1

        if cols[1] == 0:
            cols[1] = matrix.shape[1]
        col_range = np.array(np.arange(cols[0], cols[1], cols[2]), dtype=np.intp)
        cols[1] -= 1

        times = []

        self.output_message.start(self.client_id, self.session_id, "SEND_MATRIX_BLOCKS")
        self.output_message.write_matrix_id(ah.id)
        start = time.time()
        self.output_message.write_matrix_block(matrix[np.ix_(row_range, col_range)], rows, cols)
        times.append(time.time() - start)

        start = time.time()
        self.send_message()
        times.append(time.time() - start)
        start = time.time()
        self.receive_message()
        times.append(time.time() - start)
        start = time.time()
        self.input_message.read_matrix_id()
        times.append(time.time() - start)
        return times

    def get_matrix_block(self, mh, matrix, rows=[0, 0, 1], cols=[0, 0, 1]):

        if rows[1] == 0:
            rows[1] = matrix.shape[0]
        rows[1] -= 1

        if cols[1] == 0:
            cols[1] = matrix.shape[1]
        cols[1] -= 1

        times = []

        start = time.time()
        self.output_message.start(self.client_id, self.session_id, "REQUEST_MATRIX_BLOCKS")
        self.output_message.write_matrix_id(mh.id)
        self.output_message.write_long(rows[0])
        self.output_message.write_long(rows[1])
        self.output_message.write_long(rows[2])
        self.output_message.write_long(cols[0])
        self.output_message.write_long(cols[1])
        self.output_message.write_long(cols[2])
        times.append(time.time() - start)
        start = time.time()
        self.send_message()
        times.append(time.time() - start)
        start = time.time()
        self.receive_message()
        times.append(time.time() - start)
        start = time.time()
        self.input_message.read_matrix_id()
        matrix, row_range, col_range = self.input_message.read_matrix_block(matrix)
        times.append(time.time() - start)
        return matrix, times

    def get_layout(self, mh):

        col_skip = 0
        for k, v in mh.grid.items():
            col_skip += 1
            if v[0] == 1:
                break

        row_start = 0
        col_start = 0
        for k, v in mh.grid.items():
            if k == self.id:
                row_start = v[0]
                col_start = v[1]

        row_skip = int(mh.num_partitions / col_skip)

        if mh.get_layout_name(mh.layout) == "MC_MR":
            rows = [row_start, mh.num_rows, row_skip]
            cols = [col_start, mh.num_cols, col_skip]
        elif mh.get_layout_name(mh.layout) == "MC_STAR":
            rows = [0, mh.num_rows, 1]
            cols = [col_start, mh.num_cols, 1]
        elif mh.get_layout_name(mh.layout) == "MD_STAR":
            rows = [row_start, mh.num_rows, 1]
            cols = [col_start, mh.num_cols, 1]
        elif mh.get_layout_name(mh.layout) == "MR_MC":
            rows = [row_start, mh.num_rows, col_skip]
            cols = [col_start, mh.num_cols, row_skip]
        elif mh.get_layout_name(mh.layout) == "MR_STAR":
            rows = [col_start, mh.num_rows, col_skip]
            cols = [row_start, mh.num_cols, 1]
        elif mh.get_layout_name(mh.layout) == "STAR_MC":
            rows = [0, mh.num_rows, 1]
            cols = [row_start, mh.num_cols, row_skip]
        elif mh.get_layout_name(mh.layout) == "STAR_MD":
            rows = [row_start, mh.num_rows, 1]
            cols = [col_start, mh.num_cols, 1]
        elif mh.get_layout_name(mh.layout) == "STAR_MR":
            rows = [0, mh.num_rows, 1]
            cols = [col_start, mh.num_cols, col_skip]
        elif mh.get_layout_name(mh.layout) == "STAR_STAR":
            rows = [0, mh.num_rows, 1]
            cols = [0, mh.num_cols, 1]
        elif mh.get_layout_name(mh.layout) == "STAR_VC":
            rows = [0, mh.num_rows, 1]
            cols = [col_start, mh.num_cols, col_skip * row_skip]
        elif mh.get_layout_name(mh.layout) == "STAR_VR":
            rows = [row_start, mh.num_rows, 1]
            cols = [col_start, mh.num_cols, 1]
        elif mh.get_layout_name(mh.layout) == "VC_STAR":
            rows = [col_start*row_skip + row_start, mh.num_rows, col_skip*row_skip]
            cols = [0, mh.num_cols, 1]
        elif mh.get_layout_name(mh.layout) == "VR_STAR":
            rows = [row_start*col_skip + col_start, mh.num_rows, col_skip*row_skip]
            cols = [0, mh.num_cols, 1]

        return rows, cols


class WorkerClients:

    workers = []
    num_workers = 0
    times = []

    def __init__(self):
        self.num_workers = 0

    def add_workers(self, new_workers):
        for i in range(self.num_workers, len(new_workers)):
            worker = WorkerClient(new_workers[i].id, new_workers[i].hostname, new_workers[i].address, new_workers[i].port)
            self.workers.append(worker)
        self.num_workers = len(new_workers)
        self.times = np.zeros((4, self.num_workers))
        return self.num_workers

    def connect(self):
        flag = True
        for i in range(0, self.num_workers):
            flag = flag & self.workers[i].connect()
        return True

    def print(self, worker_list=[]):
        if len(worker_list) == 0:
            worker_list = self.workers

        for w in worker_list:
            print("    Worker-{0} on {1} at {2}:{3}".format(w.id, w.hostname, w.address, w.port))

    def handshake(self):
        for i in range(self.num_workers):
            self.workers[i].handshake()

    def send_matrix_blocks(self, mh, matrix):
        for i in range(self.num_workers):
            rows, cols = self.workers[i].get_layout(mh)
            self.times[:, i] = self.workers[i].send_matrix_block(mh, matrix, rows, cols)
        return self.times

    def get_matrix_blocks(self, mh, matrix):

        for i in range(self.num_workers):
            rows, cols = self.workers[i].get_layout(mh)
            matrix, self.times[:, i] = self.workers[i].get_matrix_block(mh, matrix, rows, cols)
        return matrix, self.times

    def send_test_string(self):
        for i in range(0, self.num_workers):
            self.workers[i].send_test_string()

    def request_test_string(self):
        for i in range(0, self.num_workers):
            self.workers[i].request_test_string()

    def close(self):
        for i in range(0, self.num_workers):
            self.workers[i].close()