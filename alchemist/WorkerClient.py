import socket
from .Message import Message
import numpy as np
import time


class WorkerInfo:

    id = 0
    hostname = ""
    address = ""
    port = 0

    client_id = 0
    session_id = 0


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

    def print(self):
        for i in range(0, self.num_workers):
            print("  Worker-{} on {} at {}:{}".format(self.workers[i].id, self.workers[i].hostname, self.workers[i].address, self.workers[i].port))

    def handshake(self):
        for i in range(self.num_workers):
            self.workers[i].handshake()

    def send_array_blocks(self, ah, array):
        for i in range(self.num_workers):
            rows = [self.workers[i].id-1, 0, ah.num_partitions]
            cols = [0, 0, 1]
            self.times[:, i] = self.workers[i].send_array_block(ah, array, rows, cols)
        return self.times

    def get_array_blocks(self, ah, array):
        for i in range(self.num_workers):
            rows = [self.workers[i].id-1, 0, ah.num_partitions]
            cols = [0, 0, 1]
            self.times[:, i] = self.workers[i].get_array_block(ah, array, rows, cols)
        return self.times

    def send_test_string(self):
        for i in range(0, self.num_workers):
            self.workers[i].send_test_string()

    def request_test_string(self):
        for i in range(0, self.num_workers):
            self.workers[i].request_test_string()

    def close(self):
        for i in range(0, self.num_workers):
            self.workers[i].close()


class WorkerClient:

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

    def __init__(self, id=0, hostname="0", address="0", port=0):
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
            print('Connecting to Alchemist at {0}:{1} ...'.format(*server_address))
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
            return self.reset_socket()
        except ConnectionError:
            print("ERROR: Unable to send message (ConnectionError)")
            return self.reset_socket()

    def handshake(self):
        self.output_message.start(0, 0, "HANDSHAKE")
        self.output_message.write_byte(4)
        self.output_message.write_short(1234)
        self.output_message.write_string("ABCD")
        test_array = 1.11*np.arange(3, 15).reshape((4, 3))
        self.output_message.write_array_block(test_array, [0, 4, 1], [0, 3, 1])
        self.send_message()
        self.receive_message()
        if self.input_message.read_short() == 4321:
            if self.input_message.read_string() == "DCBA":
                self.client_id = self.input_message.read_client_id()
                self.session_id = self.input_message.read_session_id()
                return True
        return False

    def send_array_block(self, ah, array, rows=[0, 0, 0], cols=[0, 0, 0]):

        if rows[1] == 0:
            rows[1] = array.shape[0]
        if rows[2] == 0:
            rows[2] = 1
        row_range = np.array(np.arange(rows[0], rows[1], rows[2]), dtype=np.intp)

        if cols[1] == 0:
            cols[1] = array.shape[1]
        if cols[2] == 0:
            cols[2] = 1
        col_range = np.array(np.arange(cols[0], cols[1], cols[2]), dtype=np.intp)

        times = []

        self.output_message.start(self.client_id, self.session_id, "SEND_ARRAY_BLOCKS")
        self.output_message.write_array_id(ah.id)
        start = time.time()
        self.output_message.write_array_block(array[np.ix_(row_range, col_range)], rows, cols)
        times.append(time.time() - start)

        start = time.time()
        self.send_message()
        times.append(time.time() - start)
        start = time.time()
        self.receive_message()
        times.append(time.time() - start)
        start = time.time()
        self.input_message.read_array_id()
        times.append(time.time() - start)
        return times

    def get_array_block(self, ah, array, rows=[0, 0, 0], cols=[0, 0, 0]):

        if rows[1] == 0:
            rows[1] = array.shape[0]
        if rows[2] == 0:
            rows[2] = 1

        if cols[1] == 0:
            cols[1] = array.shape[1]
        if cols[2] == 0:
            cols[2] = 1

        times = []

        start = time.time()
        self.output_message.start(self.client_id, self.session_id, "REQUEST_ARRAY_BLOCKS")
        self.output_message.write_array_id(ah.id)
        self.output_message.write_array_block(np.empty([0, 0]), rows, cols)
        times.append(time.time() - start)
        start = time.time()
        self.send_message()
        times.append(time.time() - start)
        start = time.time()
        self.receive_message()
        times.append(time.time() - start)
        start = time.time()
        self.input_message.read_array_id()
        self.input_message.read_array_block(array)
        times.append(time.time() - start)
        return times

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
            if self.connect:
                return True
            else:
                retries -= 1

        return False

    def reset_socket(self):
        self.close_socket()
        return self.connect()

    def close_socket(self):
        print("Closing socket")
        self.sock.close()

    def close(self):
        self.close_socket()