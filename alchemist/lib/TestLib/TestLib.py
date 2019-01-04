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
        print("Setting Alchemist session")

        self.als = als

    def get_output_parameters(self):

        return self.output_parameters

    def ready_parameters(self, name, **kwargs):

        in_args = {}

        if name == "truncated_svd":

            for key, value in kwargs.items():
                if key == "A":
                    in_args[key] = Parameter(key, "MATRIX", value)
                elif key == "rank":
                    in_args[key] = Parameter(key, "INT", value)

        return in_args

    def truncated_SVD(self, A, rank):

        in_args = {}
        in_args["A"] = Parameter("A", "MATRIX", A)
        in_args["rank"] = Parameter("rank", "INT", rank)

        self.als.display_parameters(in_args, "List of input parameters:")

        self.output_parameters = self.als.run_task(self.id, "greet", in_args)

        self.als.display_parameters(self.output_parameters, "List of output parameters:")

        U = self.output_parameters["U"].value
        V = self.output_parameters["V"].value
        S = self.output_parameters["S"].value

        return S, U, V

    def greet(self, rank):

        in_args = {}
        in_args["rank"] = Parameter("rank", "INT", rank)

        self.als.display_parameters(in_args, "List of input parameters:")

        self.output_parameters = self.als.run_task(self.id, "greet", in_args)

        self.als.display_parameters(self.output_parameters, "List of output parameters:")



