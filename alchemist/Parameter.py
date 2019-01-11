import struct


class Parameter:

    name = []
    datatype = []
    value = []

    datatypes = {"BYTE": 33,
                 "SHORT": 34,
                 "INT": 35,
                 "LONG": 36,
                 "FLOAT": 15,
                 "DOUBLE": 16,
                 "CHAR": 1,
                 "STRING": 46,
                 "MATRIX_INFO": 52}

    def __init__(self, name, datatype, value=[]):
        self.name = name
        self.datatype = datatype
        self.value = value

    def set_name(self, name):
        self.name = name

    def set_datatype(self, datatype):
        self.datatype = datatype

    def set_value(self, value):
        self.value = value

    def get_name(self):
        return self.name

    def get_datatype(self):
        return self.datatype

    def get_value(self):
        return self.value
