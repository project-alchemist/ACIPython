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

    def to_string(self, space="  "):
        if self.datatype == self.datatypes["BYTE"]:
            return "{0:17s} {1}\n{2} {3:18s} {4}".format("BYTE", self.name, space, " ", int(self.value))
        elif self.datatype == self.datatypes["CHAR"]:
            return "{0:17s} {1}\n{2} {3:18s} {4}".format("CHAR", self.name, space, " ", self.value)
        elif self.datatype == self.datatypes["SHORT"]:
            return "{0:17s} {1}\n{2} {3:18s} {4}".format("SHORT", self.name, space, " ", self.value)
        elif self.datatype == self.datatypes["INT"]:
            return "{0:17s} {1}\n{2} {3:18s} {4}".format("INT", self.name, space, " ", self.value)
        elif self.datatype == self.datatypes["LONG"]:
            return "{0:17s} {1}\n{2} {3:18s} {4}".format("LONG", self.name, space, " ", self.value)
        elif self.datatype == self.datatypes["FLOAT"]:
            return "{0:17s} {1}\n{2} {3:18s} {4}".format("FLOAT", self.name, space, " ", self.value)
        elif self.datatype == self.datatypes["DOUBLE"]:
            return "{0:17s} {1}\n{2} {3:18s} {4}".format("DOUBLE", self.name, space, " ", self.value)
        elif self.datatype == self.datatypes["STRING"]:
            return "{0:17s} {1}\n{2} {3:18s} {4}".format("STRING", self.name, space, " ", self.value)
        elif self.datatype == self.datatypes["MATRIX_ID"]:
            return "{0:17s} {1}\n{2} {3:18s} {4}".format("MATRIX ID", self.name, space, " ", self.value)
        elif self.datatype == self.datatypes["MATRIX_INFO"]:
            return "{0:17s} {1}\n{2:27s} {3}".format("MATRIX INFO", self.name, " ", self.value.to_string(display_layout=True, space=space+"{0:18s}".format(" ")))
