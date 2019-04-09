from alchemist.Parameter import Parameter
from alchemist.Alchemist import AlchemistSession


class TestLib:

    id = 0
    name = []
    als = []
    output_parameters = {}

    def __init__(self):
        self.name = "TestLib"

    def set_id(self, id):
        self.id = id

    def set_alchemist_session(self, als):
        self.als = als

    def get_output_parameters(self):

        return self.output_parameters

    def ready_parameters(self, name, **kwargs):

        in_args = {}

        if name == "truncated_svd":

            for key, value in kwargs.items():
                if key == "Aa":
                    in_args[key] = Parameter(key, Parameter.datatypes["MATRIX_ID"], value)
                elif key == "rank":
                    in_args[key] = Parameter(key, Parameter.datatypes["INT"], value)

        return in_args

    def truncated_svd(self, A, rank):

        in_args = {}
        in_args["A"] = Parameter("A", Parameter.datatypes["MATRIX_ID"], A.id)
        in_args["rank"] = Parameter("rank", Parameter.datatypes["INT"], rank)

        self.als.display_parameters(in_args, "List of input parameters:")

        self.output_parameters = self.als.run_task(self.id, "truncated_svd", in_args)

        self.als.display_parameters(self.output_parameters, "List of output parameters:")

        U = self.output_parameters["U"].value
        V = self.output_parameters["V"].value
        S = self.output_parameters["S"].value

        return S, U, V

    def greet(self, rank, bb):

        in_args = {}
        in_args["rank"] = Parameter("rank", Parameter.datatypes["INT"], rank)
        in_args["bb"] = Parameter("bb", Parameter.datatypes["DOUBLE"], bb)

        self.als.display_parameters(in_args, "List of input parameters:")

        self.output_parameters = self.als.run_task(self.id, "greet", in_args)

        self.als.display_parameters(self.output_parameters, "List of output parameters:")



