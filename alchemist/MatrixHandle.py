import numpy as np
import random as rnd


class MatrixHandle:

    id = 1

    type = 'dense'

    num_rows = 0
    num_cols = 0

    num_partitions = 0

    row_layout = []

    def __init__(self):
        self.id = 0
        self.type = 'dense'
        self.data_type = 'f'
        self.num_rows = 0
        self.num_cols = 0
        self.num_partitions = 0

    def __init__(self, id, type, num_rows, num_cols, num_partitions, row_layout):
        self.id = id
        self.type = type
        self.num_rows = num_rows
        self.num_cols = num_cols
        self.num_partitions = num_partitions
        self.row_layout = row_layout

    def set(self, id, type, num_rows, num_cols, num_partitions, row_layout):
        self.id = id
        self.type = type
        self.num_rows = num_rows
        self.num_cols = num_cols
        self.num_partitions = num_partitions
        self.row_layout = row_layout
        return self

    def set_id(self, id):
        self.id = id
        return self

    def set_type(self, type):
        self.type = type
        return self

    def set_num_rows(self, num_rows):
        self.num_rows = num_rows
        return self

    def set_num_cols(self, num_cols):
        self.num_cols = num_cols
        return self

    def set_num_partitions(self, num_partitions):
        self.num_partitions = num_partitions
        return self

    def set_layout(self, layout):
        self.layout = layout
        return self

    def fetch(self, mode='all'):
        data = 0
        if mode == 'diagonal':
            data = np.transpose(sorted((100*np.random.rand(self.num_rows, 1) + 20), reverse=True))
            # data = np.random.rand(self.num_rows, 1).tolist()
            # print("{}".format(data))

        return data

    def meta(self):
        print("ID:                    {}".format(self.id))
        print("Type:                  {}".format(self.type))
        print(" ")
        print("Number of rows:        {}".format(self.num_rows))
        print("Number of columns:     {}".format(self.num_cols))
        # print(" ")
        # print("Number of partitions:  {}".format(self.num_partitions))


