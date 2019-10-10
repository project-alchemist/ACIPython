class MatrixBlock:

    data = None
    rows = (0, 0, 1)
    cols = (0, 0, 1)

    def __init__(self, data, r=0, c=0):
        self.data = data
        self.rows = (r, r, 1)
        self.cols = (c, c, 1)

    def __init__(self, data, r=0, c=(0, 0, 1)):
        self.data = data
        self.rows = (r, r, 1)
        self.cols = c

    def __init__(self, data, r=(0, 0, 1), c=0):
        self.data = data
        self.rows = r
        self.cols = (c, c, 1)

    def __init__(self, data, r=(0, 0, 1), c=(0, 0, 1)):
        self.data = data
        self.rows = (r, r, 1)
        self.cols = (c, c, 1)

    def to_string(self, space="", print_data=False):

        data_str = "Rows: {0} {1} {2}\n".format(self.cols[0], self.cols[1], self.cols[2])
        data_str += "{0}Cols: {1} {2} {3}".format(space, self.cols[0], self.cols[1], self.cols[2])

        k = 0

        data_str += "{0}Data: ".format(space)
        if self.data.size == 0:
            data_str += "[]\n"
        else:
            if not print_data:
                data_str += "<not displaying data>\n"
            else:
                for i in range(self.rows[0], self.rows[1], self.rows[2]):
                    for j in range(self.cols[0], self.cols[1], self.cols[2]):
                        data_str += "{0} ".format(self.data[i, j])
                    data_str += "\n{0} ".format(space)

