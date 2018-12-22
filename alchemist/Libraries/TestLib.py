from ..Parameter import Parameter
from ..Alchemist import AlchemistSession


def truncated_svd(als, mh, rank):
    in_args = {}
    out_args = {}

    in_args["A"] = Parameter("A", "MATRIX", mh)
    in_args["rank"] = Parameter("rank", "INT", rank)

    out_args = als.run_task(in_args)

    S = out_args["S"].value
    U = out_args["U"].value
    V = out_args["V"].value

    return S, U, V


class TestLib:

    ID = 0
    name = []

    def __init__(self):
        self.name = "TestLib"

    def set_ID(self, ID):
        self.ID = ID

    def ready_parameters(self, name, **kwargs):

        in_args = {}
        out_args = {}

        if name == "truncated_svd":

            for key, value in kwargs.items():
                if key == "A":
                    in_args[key] = Parameter(key, "MATRIX", value)
                elif key == "rank":
                    in_args[key] = Parameter(key, "INT", value)

        return in_args


