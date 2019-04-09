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
                 "WORKER_ID": 51,
                 "WORKER_INFO": 52,
                 "MATRIX_ID": 53,
                 "MATRIX_INFO": 54}

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

    def to_string(self):
        if self.datatype == self.datatypes["BYTE"]:
            return "{2} ({0}) = {1}".format("BYTE", int(self.value), self.name)
        elif self.datatype == self.datatypes["CHAR"]:
            return "{2} ({0}) = {1}".format("CHAR", self.value, self.name)
        elif self.datatype == self.datatypes["SHORT"]:
            return "{2} ({0}) = {1}".format("SHORT", self.value, self.name)
        elif self.datatype == self.datatypes["INT"]:
            return "{2} ({0}) = {1}".format("INT", self.value, self.name)
        elif self.datatype == self.datatypes["LONG"]:
            return "{2} ({0}) = {1}".format("LONG", self.value, self.name)
        elif self.datatype == self.datatypes["FLOAT"]:
            return "{2} ({0}) = {1}".format("FLOAT", self.value, self.name)
        elif self.datatype == self.datatypes["DOUBLE"]:
            return "{2} ({0}) = {1}".format("DOUBLE", self.value, self.name)
        elif self.datatype == self.datatypes["STRING"]:
            return "{2} ({0}) = {1}".format("STRING", self.value, self.name)
        elif self.datatype == self.datatypes["MATRIX_ID"]:
            return "{2} ({0}) = {1}".format("MATRIX ID", self.value, self.name)
        elif self.datatype == self.datatypes["MATRIX_INFO"]:
            return "{2} ({0}) = \n{1}".format("MATRIX INFO", self.value.to_string(display_layout=True, space="       "), self.name)
