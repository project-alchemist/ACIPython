import numpy as np
import pyarrow as pa
import time
import math
from .Parameter import Parameter
from .MatrixHandle import MatrixHandle
from .WorkerInfo import WorkerInfo
import struct


class Message:

    header_length = 10
    max_body_length = 100000000

    commands = {"WAIT": 0,
                # Connection
                "HANDSHAKE": 1,
                "REQUEST_ID": 2,
                "CLIENT_INFO": 3,
                "SEND_TEST_STRING": 4,
                "REQUEST_TEST_STRING": 5,
                "CLOSE_CONNECTION": 6,
                # Workers
                "REQUEST_WORKERS": 11,
                "YIELD_WORKERS": 12,
                "SEND_ASSIGNED_WORKERS_INFO": 13,
                "LIST_ALL_WORKERS": 14,
                "LIST_ACTIVE_WORKERS": 15,
                "LIST_INACTIVE_WORKERS": 16,
                "LIST_ASSIGNED_WORKERS": 17,
                # Libraries
                "LIST_AVAILABLE_LIBRARIES": 21,
                "LOAD_LIBRARY": 22,
                "UNLOAD_LIBRARY": 23,
                # matrixs
                "SEND_MATRIX_INFO": 31,
                "SEND_MATRIX_LAYOUT": 32,
                "SEND_MATRIX_BLOCKS": 34,
                "REQUEST_MATRIX_BLOCKS": 36,
                # Tasks
                "RUN_TASK": 41,
                # Shutting down
                "SHUTDOWN": 99}

    datatypes = {"NONE": 0,
                 "BYTE": 33,
                 "SHORT": 34,
                 "INT": 35,
                 "LONG": 36,
                 "FLOAT": 15,
                 "DOUBLE": 16,
                 "CHAR": 1,
                 "STRING": 46,
                 "COMMAND_CODE": 48,
                 "LIBRARY_ID": 49,
                 "GROUP_ID": 50,
                 "WORKER_ID": 51,
                 "WORKER_INFO": 52,
                 "MATRIX_ID": 53,
                 "MATRIX_INFO": 54,
                 "MATRIX_BLOCK": 55,
                 "PARAMETER": 100}

    errors = {"NONE": 0,
              "INVALID_HANDSHAKE": 1,
              "INVALID_CLIENT_ID": 2,
              "INVALID_SESSION_ID": 3,
              "INCONSISTENT_DATATYPES": 4}

    message_buffer = bytearray(header_length + max_body_length)

    current_datatype = datatypes["NONE"]

    client_id = 0
    session_id = 0
    command_code = commands["WAIT"]
    error_code = errors["NONE"]
    body_length = 0

    read_pos = header_length                # for reading data
    write_pos = header_length               # for writing data

    def __init__(self):
        self.set_max_length(self.max_body_length)
        self.reset()

    def eom(self):
        return self.read_pos >= self.body_length + self.header_length

    def set_max_length(self, max_length):
        self.max_body_length = max_length - self.header_length

        self.message_buffer = bytearray(self.header_length + self.max_body_length)

    def reset(self):
        self.current_datatype = self.datatypes["NONE"]

        self.client_id = 0
        self.session_id = 0
        self.command_code = self.commands["WAIT"]
        self.error_code = self.errors["NONE"]
        self.body_length = 0

        self.write_pos = self.header_length
        self.read_pos = self.header_length

    # Utility methods
    def get_header_length(self):
        return self.header_length

    def get_command_code(self):
        return self.command_code

    def get_body_length(self):
        return self.body_length

    def get_command_name(self, v):
        return list(self.commands.keys())[list(self.commands.values()).index(v)]

    def get_error_name(self, v):
        return list(self.errors.keys())[list(self.errors.values()).index(v)]

    def get_datatype_name(self, v):
        return list(self.datatypes.keys())[list(self.datatypes.values()).index(v)]

    # Return raw byte matrix
    def get(self):
        # self.update_body_length()
        # self.update_datatype()

        return self.message_buffer[0:self.header_length + self.body_length]

    # ============================================ Reading data ============================================

    # Reading header
    def read_client_id(self):
        return int.from_bytes(self.message_buffer[0:2], 'big')

    def read_session_id(self):
        return int.from_bytes(self.message_buffer[2:4], 'big')

    def read_command_code(self):
        return self.message_buffer[4]

    def read_error_code(self):
        return self.message_buffer[5]

    def read_body_length(self):
        return int.from_bytes(self.message_buffer[6:10], 'big')

    def read_header(self):
        self.client_id = self.read_client_id()
        self.session_id = self.read_session_id()
        self.command_code = self.read_command_code()
        self.error_code = self.read_error_code()
        self.body_length = self.read_body_length()

    # Reading body
    def preview_next_datatype(self):
        return self.message_buffer[self.read_pos]

    def get_code(self):
        self.read_pos += 1
        return int.from_bytes(self.message_buffer[self.read_pos-1:self.read_pos], 'big')

    def get_byte(self):
        self.read_pos += 1
        return int.from_bytes(self.message_buffer[self.read_pos-1:self.read_pos], 'big')

    def get_char(self):
        self.read_pos += 1
        return self.message_buffer[self.read_pos-1]

    def get_short(self):
        self.read_pos += 2
        return int.from_bytes(self.message_buffer[self.read_pos-2:self.read_pos], 'big')

    def get_int(self):
        self.read_pos += 4
        return int.from_bytes(self.message_buffer[self.read_pos-4:self.read_pos], 'big')

    def get_long(self):
        self.read_pos += 8
        return int.from_bytes(self.message_buffer[self.read_pos-8:self.read_pos], 'big')

    def get_float(self):
        self.read_pos += 4
        return struct.unpack('>f', self.message_buffer[self.read_pos - 4:self.read_pos])[0]

    def get_double(self):
        self.read_pos += 8
        return struct.unpack('>d', self.message_buffer[self.read_pos - 8:self.read_pos])[0]

    def get_string(self):
        str_length = self.get_short()
        self.read_pos += str_length
        return self.message_buffer[self.read_pos - str_length:self.read_pos].decode('utf-8')

    def get_matrix_id(self):
        return self.get_short()

    def get_matrix_info(self):
        id = self.get_matrix_id()
        name = self.get_string()
        num_rows = self.get_long()
        num_cols = self.get_long()
        sparse = self.get_byte()
        layout = self.get_byte()
        num_partitions = self.get_short()
        grid = {}
        for _ in range(num_partitions):
            w = self.get_short()
            r = self.get_short()
            c = self.get_short()
            grid[w] = [r, c]

        return MatrixHandle(id, name, num_rows, num_cols, sparse, layout, num_partitions, grid)

    def get_matrix_block(self, matrix=np.zeros((1,1))):

        row_start = self.get_long()
        row_end = self.get_long()
        row_skip = self.get_long()
        col_start = self.get_long()
        col_end = self.get_long()
        col_skip = self.get_long()

        row_range = np.array(np.arange(row_start, row_end+1, row_skip), dtype=np.intp)
        col_range = np.array(np.arange(col_start, col_end+1, col_skip), dtype=np.intp)

        block_num_rows = len(row_range)
        block_num_cols = len(col_range)
        num_elements = block_num_rows * block_num_cols

        if matrix.size == 1:
            matrix = np.zeros((block_num_rows, block_num_cols))
            ixgrid = np.ix_(np.arange(block_num_rows), np.arange(block_num_cols))
        else:
            ixgrid = np.ix_(row_range, col_range)

        matrix[ixgrid] = np.frombuffer(self.message_buffer[self.read_pos:self.read_pos + 8*num_elements], dtype=np.float64).reshape((block_num_rows, block_num_cols))
        self.read_pos += 8 * num_elements

        return matrix, row_range, col_range

    def get_worker_id(self):
        return self.get_short()

    def get_worker_info(self):
        worker_id = self.get_worker_id()
        hostname = self.get_string()
        address = self.get_string()
        port = self.get_short()
        group_id = self.get_short()

        return WorkerInfo(worker_id, hostname, address, port, group_id)

    def get_parameter(self):

        name = self.read_string()
        parameter_type = self.preview_next_datatype()

        if parameter_type == self.datatypes["BYTE"]:
            v = self.read_byte()
        elif parameter_type == self.datatypes["CHAR"]:
            v = self.read_char()
        elif parameter_type == self.datatypes["SHORT"]:
            v = self.read_short()
        elif parameter_type == self.datatypes["INT"]:
            v = self.read_int()
        elif parameter_type == self.datatypes["LONG"]:
            v = self.read_long()
        elif parameter_type == self.datatypes["FLOAT"]:
            v = self.read_float()
        elif parameter_type == self.datatypes["DOUBLE"]:
            v = self.read_double()
        elif parameter_type == self.datatypes["STRING"]:
            v = self.read_string()
        elif parameter_type == self.datatypes["MATRIX_ID"]:
            v = self.read_matrix_id()
        elif parameter_type == self.datatypes["MATRIX_INFO"]:
            v = self.read_matrix_info()

        return Parameter(name, parameter_type, v)

    def read_byte(self):

        if self.get_code() != self.datatypes["BYTE"]:
            print("Actual datatype does not match expected datatype BYTE")
            return 0
        else:
            return self.get_byte()

    def read_char(self):

        if self.get_code() != self.datatypes["CHAR"]:
            print("Actual datatype does not match expected datatype CHAR")
            return 0
        else:
            return self.get_char()

    def read_short(self):

        if self.get_code() != self.datatypes["SHORT"]:
            print("Actual datatype does not match expected datatype SHORT")
            return 0
        else:
            return self.get_short()

    def read_int(self):

        if self.get_code() != self.datatypes["INT"]:
            print("Actual datatype does not match expected datatype INT")
            return 0
        else:
            return self.get_int()

    def read_long(self):

        if self.get_code() != self.datatypes["LONG"]:
            print("Actual datatype does not match expected datatype LONG")
            return 0
        else:
            return self.get_long()

    def read_float(self):

        if self.get_code() != self.datatypes["FLOAT"]:
            print("Actual datatype does not match expected datatype FLOAT")
            return 0.0
        else:
            return self.get_float()

    def read_double(self):

        if self.get_code() != self.datatypes["DOUBLE"]:
            print("Actual datatype does not match expected datatype DOUBLE")
            return 0.0
        else:
            return self.get_double()

    def read_string(self):

        if self.get_code() != self.datatypes["STRING"]:
            return "Actual datatype does not match expected datatype STRING"
        else:
            return self.get_string()

    def read_library_id(self):

        if self.get_code() != self.datatypes["LIBRARY_ID"]:
            message = "Actual datatype does not match expected datatype LIBRARY ID"
            return 0
        else:
            return self.get_byte()

    def read_matrix_id(self):

        if self.get_code() != self.datatypes["MATRIX_ID"]:
            message = "Actual datatype does not match expected datatype MATRIX ID"
            return 0
        else:
            return self.get_short()

    def read_matrix_info(self):

        if self.get_code() != self.datatypes["MATRIX_INFO"]:
            message = "Actual datatype does not match expected datatype MATRIX INFO"
            return 0
        else:
            return self.get_matrix_info()

    def read_matrix_block(self, matrix=np.zeros((1,1))):

        if self.get_code() != self.datatypes["MATRIX_BLOCK"]:
            message = "Actual datatype does not match expected datatype MATRIX BLOCK"
            return 0
        else:
            return self.get_matrix_block(matrix)

    def read_worker_id(self):

        if self.get_code() != self.datatypes["WORKER_ID"]:
            message = "Actual datatype does not match expected datatype WORKER ID"
            return 0
        else:
            return self.get_short()

    def read_worker_info(self):

        if self.get_code() != self.datatypes["WORKER_INFO"]:
            message = "Actual datatype does not match expected datatype WORKER INFO"
            return 0
        else:
            return self.get_worker_info()

    def read_parameter(self):

        if self.get_code() != self.datatypes["PARAMETER"]:
            print("Actual datatype does not match expected datatype PARAMETER")
            return []
        else:
            return self.get_parameter()

    # ============================================ Writing data ============================================

    def start(self, client_id, session_id, command_code, error_code="NONE"):
        self.message_buffer[0:2] = client_id.to_bytes(2, 'big')
        self.message_buffer[2:4] = session_id.to_bytes(2, 'big')
        self.message_buffer[4] = self.commands[command_code]
        self.message_buffer[5] = self.errors[error_code]

        return self

    def add_header(self, header):
        self.message_buffer[0:self.header_length] = header
        self.read_header()

        return self
        
    def add_packet(self, packet):
        self.message_buffer[self.write_pos:self.write_pos + len(packet)] = packet
        self.write_pos += len(packet)

        return self

    def put_datatype(self, name):
        self.message_buffer[self.write_pos] = self.datatypes[name].to_bytes(1, 'big')[0]
        self.write_pos += 1

        return self

    def put_byte(self, value):
        self.message_buffer[self.write_pos] = value.to_bytes(1, 'big')[0]
        self.write_pos += 1

        return self

    def put_char(self, value):
        self.message_buffer[self.write_pos] = value.encode('utf-8')[0]
        self.write_pos += 1

        return self

    def put_short(self, value):
        self.message_buffer[self.write_pos:self.write_pos+2] = value.to_bytes(2, 'big')
        self.write_pos += 2

        return self

    def put_int(self, value):
        self.message_buffer[self.write_pos:self.write_pos+4] = value.to_bytes(4, 'big')
        self.write_pos += 4

        return self

    def put_long(self, value):
        self.message_buffer[self.write_pos:self.write_pos+8] = value.to_bytes(8, 'big')
        self.write_pos += 8

        return self

    def put_float(self, value):
        self.message_buffer[self.write_pos:self.write_pos+4] = struct.pack('>f', value)
        self.write_pos += 4

        return self

    def put_double(self, value):
        self.message_buffer[self.write_pos:self.write_pos+8] = struct.pack('>d', value)
        self.write_pos += 8

        return self

    def put_string(self, s):
        self.put_short(len(s))
        self.message_buffer[self.write_pos:self.write_pos+len(s)] = s.encode('utf-8')
        self.write_pos += len(s)

        return self

    def put_matrix_id(self, v):
        self.message_buffer[self.write_pos:self.write_pos+2] = v.to_bytes(2, 'big')
        self.write_pos += 2

        return self

    def put_library_id(self, v):
        self.message_buffer[self.write_pos] = v.to_bytes(1, 'big')[0]
        self.write_pos += 1

        return self

    def put_matrix_block(self, block, rows, cols):

        self.put_long(rows[0])
        self.put_long(rows[1])
        self.put_long(rows[2])
        self.put_long(cols[0])
        self.put_long(cols[1])
        self.put_long(cols[2])

        if block.size > 0:
            # start = time.time()
            # self.message_buffer[self.write_pos:self.write_pos + 8*block.size] = block.tobytes('C')
            # # self.message_buffer[self.write_pos:self.write_pos + block.size] = block.view(dtype=np.uint8)
            # send_time = time.time() - start
            # print("Send time 3 {0}".format(send_time))
            # start = time.time()
            # self.message_buffer[self.write_pos:self.write_pos + 8*block.size] = pa.serialize(block).to_buffer()
            # send_time = time.time() - start
            # print("Send time 2 {0}".format(send_time))
            self.message_buffer[self.write_pos:self.write_pos + 8 * block.size] = block.tobytes('C')
            self.write_pos += 8 * block.size

        return self

    def put_parameter(self, p):

        self.write_string(p.name)
        if p.datatype == self.datatypes["BYTE"]:
            self.write_byte(p.value)
        elif p.datatype == self.datatypes["CHAR"]:
            self.write_char(p.value)
        elif p.datatype == self.datatypes["SHORT"]:
            self.write_short(p.value)
        elif p.datatype == self.datatypes["INT"]:
            self.write_int(p.value)
        elif p.datatype == self.datatypes["LONG"]:
            self.write_long(p.value)
        elif p.datatype == self.datatypes["FLOAT"]:
            self.write_float(p.value)
        elif p.datatype == self.datatypes["DOUBLE"]:
            self.write_double(p.value)
        elif p.datatype == self.datatypes["STRING"]:
            self.write_string(p.value)
        elif p.datatype == self.datatypes["MATRIX_ID"]:
            self.write_matrix_id(p.value)

    def write_byte(self, value):
        self.put_datatype("BYTE")
        self.put_byte(value)

        return self

    def write_char(self, value):
        self.put_datatype("CHAR")
        self.put_char(value)

        return self

    def write_short(self, value):
        self.put_datatype("SHORT")
        self.put_short(value)

        return self

    def write_int(self, value):
        self.put_datatype("INT")
        self.put_int(value)

        return self

    def write_long(self, value):
        self.put_datatype("LONG")
        self.put_long(value)

        return self

    def write_float(self, value):
        self.put_datatype("FLOAT")
        self.put_float(value)

        return self

    def write_double(self, value):
        self.put_datatype("DOUBLE")
        self.put_double(value)

        return self

    def write_string(self, value):
        self.put_datatype("STRING")
        self.put_string(value)

        return self

    def write_matrix_id(self, value):
        self.put_datatype("MATRIX_ID")
        self.put_matrix_id(value)

        return self

    def write_matrix_block(self, block, rows, cols):
        self.put_datatype("MATRIX_BLOCK")
        self.put_matrix_block(block, rows, cols)

        return self

    def write_library_id(self, value):
        self.put_datatype("LIBRARY_ID")
        self.put_library_id(value)

        return self

    def write_parameter(self, p):
        self.put_datatype("PARAMETER")
        self.put_parameter(p)

        return self

    # ==================================================================================================

    def update_body_length(self):
        self.body_length = self.write_pos - self.header_length
        self.message_buffer[6:10] = self.body_length.to_bytes(4, 'big')

    def reset_write_position(self):
        self.write_pos = self.header_length

    def reset_read_position(self):
        self.read_pos = self.header_length

    def finish(self):
        self.update_body_length()
        self.read_header()
        self.reset_write_position()

    # ========================================================================================

    def print(self):

        space = "                   "

        self.read_header()

        print(" ")
        print("{} ==================================================================".format(space))
        print("{}  Client ID:            {}".format(space, self.client_id))
        print("{}  Session ID:           {}".format(space, self.session_id))
        print("{}  Command code:         {} ({})".format(space, self.command_code, self.get_command_name(self.command_code)))
        print("{}  Error code:           {} ({})".format(space, self.error_code, self.get_error_name(self.error_code)))
        print("{}  Message body length:  {}".format(space, self.body_length))
        print("{} -----------------------------------------------------------------".format(space))

        while not self.eom():
            next_datatype = self.preview_next_datatype()

            if next_datatype == self.datatypes["BYTE"]:
                data = " {0:18s}    {1}".format("BYTE", int(self.read_byte()))
            elif next_datatype == self.datatypes["CHAR"]:
                data = " {0:18s}    {1}".format("CHAR", self.read_char())
            elif next_datatype == self.datatypes["SHORT"]:
                data = " {0:18s}    {1}".format("SHORT", self.read_short())
            elif next_datatype == self.datatypes["INT"]:
                data = " {0:18s}    {1}".format("INT", self.read_int())
            elif next_datatype == self.datatypes["LONG"]:
                data = " {0:18s}    {1}".format("LONG", self.read_long())
            elif next_datatype == self.datatypes["FLOAT"]:
                data = " {0:18s}    {1}".format("FLOAT", self.read_float())
            elif next_datatype == self.datatypes["DOUBLE"]:
                data = " {0:18s}    {1}".format("DOUBLE", self.read_double())
            elif next_datatype == self.datatypes["STRING"]:
                data = " {0:18s}    {1}".format("STRING", self.read_string())
            elif next_datatype == self.datatypes["LIBRARY_ID"]:
                data = " {0:18s}    {1}".format("LIBRARY ID", self.read_library_id())
            elif next_datatype == self.datatypes["MATRIX_ID"]:
                data = " {0:18s}    {1}".format("MATRIX ID", self.read_matrix_id())
            elif next_datatype == self.datatypes["MATRIX_INFO"]:
                data = " {0:18s}    \n{1}".format("MATRIX INFO", self.read_matrix_info().to_string(display_layout=True, space=space + "{0:23s}".format(" ")))
            elif next_datatype == self.datatypes["MATRIX_BLOCK"]:
                block, row_range, col_range = self.read_matrix_block()
                block_string = "{0}x{1} | {2} | {3}".format(block.shape[0], block.shape[1], row_range, col_range)
                data = " {0:18s}    {1}".format("MATRIX BLOCK", block_string)
            elif next_datatype == self.datatypes["WORKER_ID"]:
                data = " {0:18s}    {1}".format("WORKER ID", self.read_worker_id())
            elif next_datatype == self.datatypes["WORKER_INFO"]:
                data = " {0:18s}    \n{1}".format("WORKER INFO", self.read_worker_info().to_string(space + "{0:23s}".format(" ")))
            elif next_datatype == self.datatypes["PARAMETER"]:
                data = " {0:18s}    {1}".format("PARAMETER", self.read_parameter().to_string())

            print("{} {}".format(space, data))

        print("{} ==================================================================".format(space))

        self.reset_read_position()
