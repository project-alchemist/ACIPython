import numpy as np
import struct


class Message:

    header_length = 9
    max_body_length = 1000

    commands = {"WAIT": 0, "HANDSHAKE": 1, "REQUEST_ID": 2, "CLIENT_INFO": 3, "SEND_TEST_STRING": 4,
                "REQUEST_TEST_STRING": 5, "REQUEST_WORKERS": 6, "YIELD_WORKERS": 7,
                "SEND_ASSIGNED_WORKERS_INFO": 8, "LIST_ALL_WORKERS": 9, "LIST_ACTIVE_WORKERS": 10,
                "LIST_INACTIVE_WORKERS": 11, "LIST_ASSIGNED_WORKERS": 12, "LOAD_LIBRARY": 13,
                "RUN_TASK": 14, "UNLOAD_LIBRARY": 15, "MATRIX_INFO": 16, "MATRIX_LAYOUT": 17,
                "SEND_MATRIX_BLOCKS": 18, "REQUEST_MATRIX_BLOCKS":19, "SHUT_DOWN": 20}

    datatypes = {"NONE": 0, "BYTE": 18, "SHORT": 34, "INT": 35, "LONG": 36, "FLOAT": 15, "DOUBLE": 16,
                 "CHAR": 1, "STRING": 46, "COMMAND_CODE": 47}

    message_buffer = bytearray(header_length)

    current_datatype = datatypes["NONE"]
    current_datatype_count = 0
    current_datatype_count_max = 0
    current_datatype_count_pos = header_length

    client_id = 0
    session_id = 0
    command_code = commands["WAIT"]
    body_length = 0

    read_pos = header_length                # for reading data
    write_pos = header_length               # for writing data

    def __init__(self):
        self.set_max_length(1000000)
        self.reset()

    def eom(self):
        print((self.read_pos, self.body_length))
        print(self.read_pos > self.body_length)
        return self.read_pos > self.body_length

    def set_max_length(self, max_length):
        self.max_body_length = max_length - self.header_length

        self.message_buffer = bytearray(self.header_length + self.max_body_length)

    def reset(self):
        self.current_datatype = self.datatypes["NONE"]
        self.current_datatype_count = 0
        self.current_datatype_count_max = 0
        self.current_datatype_count_pos = self.header_length

        self.client_id = 0
        self.session_id = 0
        self.command_code = self.commands["WAIT"]

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

    def get_datatype_name(self, v):
        return list(self.datatypes.keys())[list(self.datatypes.values()).index(v)]

    # Return raw byte array
    def get(self):
        self.update_body_length()
        self.update_datatype()

        return self.message_buffer[0:self.header_length + self.body_length]

    # Reading data
    def read_client_id(self):
        return int.from_bytes(self.message_buffer[0:2], 'big')

    def read_session_id(self):
        return int.from_bytes(self.message_buffer[2:4], 'big')

    def read_command_code(self):
        return self.message_buffer[4]

    def read_body_length(self):
        return int.from_bytes(self.message_buffer[5:9], 'big')

    def read_header(self):
        self.client_id = self.read_client_id()
        self.session_id = self.read_session_id()
        self.command_code = self.read_command_code()
        self.body_length = self.read_body_length()
        self.read_pos = self.header_length
        self.write_pos = self.header_length

    def read_next_datatype(self):
        self.current_datatype = self.message_buffer[self.read_pos]
        self.current_datatype_count = 0
        self.current_datatype_count_max = int.from_bytes(self.message_buffer[self.read_pos+1:self.read_pos+5], 'big')

        self.read_pos += 5

    def get_current_datatype(self):
        return self.current_datatype

    def get_current_datatype_name(self):
        return self.get_datatype_name(self.current_datatype)

    def get_current_datatype_count(self):
        return self.current_datatype_count_max

    def read_byte(self):
        if self.read_pos == self.header_length or self.current_datatype_count == self.current_datatype_count_max:
            self.read_next_datatype()
        self.read_pos += 1
        self.current_datatype_count += 1
        return self.message_buffer[self.read_pos-1]

    def read_char(self):
        if self.read_pos == self.header_length or self.current_datatype_count == self.current_datatype_count_max:
            self.read_next_datatype()
        self.read_pos += 1
        self.current_datatype_count += 1
        return self.message_buffer[self.read_pos-1]

    def read_short(self):
        if self.read_pos == self.header_length or self.current_datatype_count == self.current_datatype_count_max:
            self.read_next_datatype()
        self.read_pos += 2
        self.current_datatype_count += 1
        return int.from_bytes(self.message_buffer[self.read_pos-2:self.read_pos], 'big')

    def read_int(self):
        if self.read_pos == self.header_length or self.current_datatype_count == self.current_datatype_count_max:
            self.read_next_datatype()
        self.read_pos += 4
        self.current_datatype_count += 1
        return int.from_bytes(self.message_buffer[self.read_pos-4:self.read_pos], 'big')

    def read_long(self):
        if self.read_pos == self.header_length or self.current_datatype_count == self.current_datatype_count_max:
            self.read_next_datatype()
        self.read_pos += 8
        self.current_datatype_count += 1
        return int.from_bytes(self.message_buffer[self.read_pos-8:self.read_pos], 'big')

    def read_float(self):
        if self.read_pos == self.header_length or self.current_datatype_count == self.current_datatype_count_max:
            self.read_next_datatype()
        self.read_pos += 4
        self.current_datatype_count += 1
        return struct.unpack('>f', self.message_buffer[self.read_pos-4:self.read_pos])[0]

    def read_double(self):
        if self.read_pos == self.header_length or self.current_datatype_count == self.current_datatype_count_max:
            self.read_next_datatype()
        self.read_pos += 8
        self.current_datatype_count += 1
        return struct.unpack('>d', self.message_buffer[self.read_pos-8:self.read_pos])[0]

    def read_string(self):
        if self.read_pos == self.header_length or self.current_datatype_count == self.current_datatype_count_max:
            self.read_next_datatype()
        self.read_pos += self.current_datatype_count_max
        self.current_datatype_count = self.current_datatype_count_max
        return self.message_buffer[self.read_pos - self.current_datatype_count_max:self.read_pos].decode('utf-8')

    # Writing data
    def start(self, client_id, session_id, command):
        self.message_buffer[0:2] = client_id.to_bytes(2, 'big')
        self.message_buffer[2:4] = session_id.to_bytes(2, 'big')
        self.message_buffer[4] = self.commands[command]

    def add_header(self, header):
        self.message_buffer[0:self.header_length] = header
        self.read_header()
        
    def add_packet(self, packet):
        self.message_buffer[self.write_pos:self.write_pos + len(packet)] = packet
        self.write_pos + len(packet)

    def put_byte(self, value, pos):
        self.message_buffer[pos] = value.to_bytes(1, 'big')[0]

    def write_byte(self, value):
        self.check_datatype("BYTE")
        self.put_byte(value, self.write_pos)

        self.write_pos += 1

    def put_char(self, value, pos):
        self.message_buffer[pos] = value.encode('utf-8')[0]

    def write_char(self, value):
        self.check_datatype("CHAR")
        self.put_char(value, self.write_pos)

        self.write_pos += 1

    def put_short(self, value, pos):
        self.message_buffer[pos:pos+2] = value.to_bytes(2, 'big')

    def write_short(self, value):
        self.check_datatype("SHORT")
        self.put_short(value, self.write_pos)

        self.write_pos += 2

    def put_int(self, value, pos):
        self.message_buffer[pos:pos+4] = value.to_bytes(4, 'big')

    def write_int(self, value):
        self.check_datatype("INT")
        self.put_int(value, self.write_pos)

        self.write_pos += 4

    def put_long(self, value, pos):
        self.message_buffer[pos:pos+8] = value.to_bytes(8, 'big')

    def write_long(self, value):
        self.check_datatype("LONG")
        self.put_long(value, self.write_pos)

        self.write_pos += 8

    def put_float(self, value, pos):
        self.message_buffer[pos:pos+4] = struct.pack('>f', value)

    def write_float(self, value):
        self.check_datatype("FLOAT")
        self.put_float(value, self.write_pos)

        self.write_pos += 4

    def put_double(self, value, pos):
        self.message_buffer[pos:pos+8] = struct.pack('>d', value)

    def write_double(self, value):
        self.check_datatype("DOUBLE")
        self.put_double(value, self.write_pos)

        self.write_pos += 8

    def put_string(self, value, pos, length):
        self.message_buffer[pos:pos+length] = value.encode('utf-8')

    def write_string(self, value):
        self.check_datatype("STRING")
        self.current_datatype_count = len(value)
        self.put_string(value, self.write_pos, self.current_datatype_count)

        self.write_pos += self.current_datatype_count
        self.current_datatype = self.datatypes["NONE"]

    # ========================================================================================

    def check_datatype(self, t):
        if self.current_datatype != self.datatypes[t]:
            self.current_datatype = self.datatypes[t]

            if self.current_datatype_count_pos > self.header_length:
                self.put_int(self.current_datatype_count, self.current_datatype_count_pos)

            self.put_byte(self.current_datatype, self.write_pos)

            self.current_datatype_count = 1
            self.current_datatype_count_pos = self.write_pos + 1
            self.write_pos += 5
        else:
            self.current_datatype_count += 1

    def update_body_length(self):
        self.body_length = self.write_pos - self.header_length
        self.message_buffer[5:9] = self.body_length.to_bytes(4, 'big')

    def update_datatype(self):
        if self.current_datatype_count_pos > self.header_length:
            self.put_int(self.current_datatype_count, self.current_datatype_count_pos)

    def finish(self):
        self.update_body_length()
        self.update_datatype()

    # ========================================================================================

    def print(self):

        space = "                        "

        temp_client_id = int.from_bytes(self.message_buffer[0:2], 'big')
        temp_session_id = int.from_bytes(self.message_buffer[2:4], 'big')
        temp_command_code = self.message_buffer[4]
        temp_body_length = int.from_bytes(self.message_buffer[5:9], 'big')

        print(" ")
        print("{} ==============================================".format(space))
        print("{} Client ID:           {}".format(space, temp_client_id))
        print("{} Session ID:          {}".format(space, temp_session_id))
        print("{} Command code:        {} ({})".format(space, temp_command_code, self.get_command_name(temp_command_code)))
        print("{} Message body length: {}".format(space, temp_body_length))
        print("{} ----------------------------------------------".format(space))

        i = self.header_length

        while i < self.header_length + temp_body_length:

            data_array_type = self.get_datatype_name(self.message_buffer[i])
            data_array_length = int.from_bytes(self.message_buffer[i+1:i+5], 'big')

            print("{} Datatype (length):   {} ({})".format(space, data_array_type, data_array_length))

            data = ""
            i += 5

            if data_array_type == "STRING":
                data += self.message_buffer[i:i+data_array_length].decode('utf-8')
                i += data_array_length
            else:
                for j in range(0, data_array_length):
                    if data_array_type == "BYTE":
                        data += str(self.message_buffer[i])
                    elif data_array_type == "CHAR":
                        data += str(self.message_buffer[i])
                    elif data_array_type == "SHORT":
                        data += str(int.from_bytes(self.message_buffer[i:i+2], 'big'))
                    elif data_array_type == "INT":
                        data += str(int.from_bytes(self.message_buffer[i:i+4], 'big'))
                    elif data_array_type == "LONG":
                        data += str(int.from_bytes(self.message_buffer[i:i+8], 'big'))
                    elif data_array_type == "FLOAT":
                        data += str(struct.unpack('>f', self.message_buffer[i:i+4])[0])
                    elif data_array_type == "DOUBLE":
                        data += str(struct.unpack('>d', self.message_buffer[i:i+8])[0])

                    data += " "

                    datatype_length = {"BYTE": 1, "CHAR": 1, "SHORT": 2, "INT": 4, "LONG": 8, "FLOAT": 4, "DOUBLE": 8}.get(data_array_type, 4)
                    i += datatype_length

            print("{} Data:                {}".format(space, data))
            if i < self.header_length + temp_body_length:
                print(" ")

        print("{} ==============================================".format(space))
