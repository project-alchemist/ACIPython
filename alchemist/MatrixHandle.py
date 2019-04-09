class MatrixHandle:

    layouts = {"MC_MR": 0,
               "MC_STAR": 1,
               "MD_STAR": 2,
               "MR_MC": 3,
               "MR_STAR": 4,
               "STAR_MC": 5,
               "STAR_MD": 6,
               "STAR_MR": 7,
               "STAR_STAR": 8,
               "STAR_VC": 9,
               "STAR_VR": 10,
               "VC_STAR": 11,
               "VR_STAR": 12,
               "CIRC_CIRC": 13}

    id = 0
    name = ""
    num_rows = 0
    num_cols = 0
    sparse = 0
    layout = 0
    num_partitions = 0
    grid = {}

    def __init__(self, id=0, name="", num_rows=0, num_cols=0, sparse=0, layout=0, num_partitions=0, grid={}):
        self.id = id
        self.name = name
        self.num_rows = num_rows
        self.num_cols = num_cols
        self.sparse = sparse
        self.layout = layout
        self.num_partitions = num_partitions
        self.grid = grid

    def set(self, id, name="", num_rows=0, num_cols=0, sparse=0, layout=0, num_partitions=0, grid={}):
        self.id = id
        self.name = name
        self.num_rows = num_rows
        self.num_cols = num_cols
        self.sparse = sparse
        self.layout = layout
        self.num_partitions = num_partitions
        self.grid = grid
        return self

    def set_id(self, id):
        self.id = id
        return self

    def set_name(self, name):
        self.name = name
        return self

    def set_num_rows(self, num_rows):
        self.num_rows = num_rows
        return self

    def set_num_cols(self, num_cols):
        self.num_cols = num_cols
        return self

    def set_sparse(self, sparse):
        self.sparse = sparse
        return self

    def set_layout(self, layout):
        self.layout = layout
        return self

    def set_num_partitions(self, num_partitions):
        self.num_partitions = num_partitions
        return self

    def fetch(self, mode='all'):
        data = 0
        # if mode == 'diagonal':
            # data = np.transpose(sorted((100*np.random.rand(self.num_rows, 1) + 20), reverse=True))
            # data = np.random.rand(self.num_rows, 1).tolist()
            # print("{}".format(data))

        return data

    def get_layout_name(self, l):
        return list(self.layouts.keys())[list(self.layouts.values()).index(l)]

    def to_string(self, display_layout=False, space=""):
        meta = "{0} Name:                  {1}\n".format(space, self.name)
        meta += "{0} ID:                    {1}\n\n".format(space, self.id)
        meta += "{0} Number of rows:        {1}\n".format(space, self.num_rows)
        meta += "{0} Number of columns:     {1}\n\n".format(space, self.num_cols)
        meta += "{0} Sparse:                {1}\n".format(space, self.sparse)
        meta += "{0} Layout:                {1}\n".format(space, self.get_layout_name(self.layout))
        meta += "{0} Number of partitions:  {1}\n".format(space, self.num_partitions)
        if display_layout:
            meta += "{0} Worker assignments:    {1}".format(space, self.grid)

        return meta


