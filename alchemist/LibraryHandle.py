from .MatrixHandle import MatrixHandle


class LibraryHandle:

    ID = 0
    name = " "
    path = " "

    def __init__(self, ID, name, path=""):
        self.ID = ID
        self.name = name
        self.path = path


class TestLib(LibraryHandle):

    def __init__(self, name, path):
        self.name = name
        self.path = path

    def truncated_svd(self, A, k):
        print("Computing rank-{} SVD of Matrix {}".format(k, A.id))
        S = MatrixHandle()
        S.set(25, 'diagonal', 20, 20, 2)
        U = MatrixHandle()
        U.set(26, 'dense', 1200000, 20, 2)
        V = MatrixHandle()
        V.set(27, 'dense', 1200000, 20, 2)
        return S, U, V