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

        # self.als.display_parameters(self.output_parameters, "List of output parameters:")

        U = self.output_parameters["U"].value
        V = self.output_parameters["V"].value
        S = self.output_parameters["S"].value

        return S, U, V

    def greet(self, in_byte, in_char, in_short, in_int, in_long, in_float, in_double, in_string):

        in_args = {}
        in_args["in_byte"] = Parameter("in_byte", Parameter.datatypes["BYTE"], in_byte)
        in_args["in_char"] = Parameter("in_char", Parameter.datatypes["CHAR"], in_char)
        in_args["in_short"] = Parameter("in_short", Parameter.datatypes["SHORT"], in_short)
        in_args["in_int"] = Parameter("in_int", Parameter.datatypes["INT"], in_int)
        in_args["in_long"] = Parameter("in_long", Parameter.datatypes["LONG"], in_long)
        in_args["in_float"] = Parameter("in_float", Parameter.datatypes["FLOAT"], in_float)
        in_args["in_double"] = Parameter("in_double", Parameter.datatypes["DOUBLE"], in_double)
        in_args["in_string"] = Parameter("in_string", Parameter.datatypes["STRING"], in_string)

        self.als.display_parameters(in_args, "List of input parameters:")

        self.output_parameters = self.als.run_task(self.id, "greet", in_args)

        self.als.display_parameters(self.output_parameters, "List of output parameters:")



