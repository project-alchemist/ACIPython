import socket
from .Message import Message


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

    def __init__(self):
        self.num_workers = 0

    def set_workers(self, worker_details):
        self.num_workers = len(worker_details)
        for i in range(0, self.num_workers):
            worker = WorkerClient(worker_details[i].id, worker_details[i].hostname, worker_details[i].address, worker_details[i].port)
            self.workers.append(worker)

    def connect(self):
        flag = True
        for i in range(0, self.num_workers):
            flag = flag & self.workers[i].connect()
        return True

    def print(self):
        for i in range(0, self.num_workers):
            print("  Worker-{} on {} at {}:{}".format(self.workers[i].id, self.workers[i].hostname, self.workers[i].address, self.workers[i].port))

    def handshake(self):
        for i in range(0, self.num_workers):
            self.workers[i].handshake()

    def send_blocks(self, mh, data, row_start=0):
        for i in range(0, self.num_workers):
            self.workers[i].send_blocks(mh, data, row_start)

    def get_blocks(self, mh, data, rows, cols):
        for i in range(0, self.num_workers):
            self.workers[i].get_blocks(mh, data, rows, cols)

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

    input_message = Message()
    output_message = Message()

    def __init__(self, id=0, hostname="0", address="0", port=0):
        self.id = id
        self.hostname = hostname
        self.address = address
        self.port = port

        self.sock = []

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
            return self.reset_socket()
        except ConnectionError:
            print("ERROR: Unable to send message (ConnectionError)")
            return self.reset_socket()

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

    def send_blocks(self, mh, data, row_start=0):

        sh = data.shape

        self.output_message.start(self.client_id, self.session_id, "SEND_MATRIX_BLOCKS")
        self.output_message.write_short(mh.id)
        for i in range(0, sh[0]):
            if mh.row_layout[row_start+i] == self.id:
                self.output_message.write_long(row_start+i)
                self.output_message.write_long(row_start+i)
                self.output_message.write_long(0)
                self.output_message.write_long(sh[1]-1)
                for j in range(0, sh[1]):
                    self.output_message.write_double(data[i, j])

        self.send_message()
        self.receive_message()

        self.input_message.read_short()
        print("Alchemist worker " + str(self.id) + " received " + str(self.input_message.read_int()) + " blocks")

    def get_blocks(self, mh, data, rows, cols):
        self.output_message.start(self.client_id, self.session_id, "REQUEST_MATRIX_BLOCKS")
        self.output_message.write_short(mh.id)
        for i in rows:
            if mh.row_layout[i] == self.id:
                self.output_message.write_long(i)
                self.output_message.write_long(i)
                self.output_message.write_long(cols[0])
                self.output_message.write_long(cols[-1])

        self.send_message()
        self.receive_message()

        self.input_message.read_short()

        while True:
            row_start = self.input_message.read_long()
            row_end   = self.input_message.read_long()
            col_start = self.input_message.read_long()
            col_end   = self.input_message.read_long()

            for i in range(row_start, row_end+1):
                for j in range(col_start, col_end+1):
                    data[rows.index(i), cols.index(j)] = self.input_message.read_double()

            if self.input_message.eom():
                break

        return data

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