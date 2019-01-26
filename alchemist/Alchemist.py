from .DriverClient import DriverClient
from .WorkerClient import WorkerClients
import time
import h5py
import os
import importlib
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


class AlchemistSession:

    driver = []
    workers = []
    libraries = dict()

    workers_connected = False

    def __init__(self):
        print("Starting Alchemist session ... ", end="", flush=True)
        self.driver = DriverClient()
        self.workers = WorkerClients()
        self.workers_connected = False
        print("ready")

    def __del__(self):
        print("Ending Alchemist session")
        self.close()

    def namestr(self, obj, namespace):
        return [name for name in namespace if namespace[name] is obj]

    def read_from_hdf5(self, filename):
        print("Loaded " + filename)
        return h5py.File(filename, 'r')

    def send_array(self, array, print_times=False):
        max_block_rows = 100
        max_block_cols = 20000

        (num_rows, num_cols) = array.shape

        print("Sending array info to Alchemist ... ", end="", flush=True)
        start = time.time()
        ah = self.get_array_handle(array)
        end = time.time()
        print("done ({0:.4e}s)".format(end - start))

        print("Sending array data to Alchemist ... ", end="", flush=True)
        start = time.time()
        times = self.workers.send_array_blocks(ah, array)
        end = time.time()
        print("done ({0:.4e}s)".format(end - start))
        if print_times:
            self.print_times(times, name=ah.name)
        #     self.driver.send_block(mh, block)

        return ah

    def fetch_array(self, ah, print_times=False):

        array = np.zeros((ah.num_rows, ah.num_cols))

        print("Fetching data for array {0} from Alchemist ... ".format(ah.name), end="", flush=True)
        start = time.time()
        times = self.workers.get_array_blocks(ah, array)
        end = time.time()
        print("done ({0:.4e}s)".format(end - start))
        if print_times:
            self.print_times(times, name=ah.name)
        return array

    def print_times(self, times, name=" ", spacing="  "):
        print("")
        if name is "":
            print("Data transfer times breakdown")
        else:
            print("Data transfer times breakdown for array {}".format(name))
        print("{}---------------------------------------------------------------------------------------------------------------".format(spacing))
        print("{}  Worker  |   Serialization time   |       Send time        |      Receive time      |  Deserialization time  ".format(spacing))
        print("{}---------------------------------------------------------------------------------------------------------------".format(spacing))
        for i in range(self.workers.num_workers):
            print("{0}    {1:3d}   |       {2:.4e}       |       {3:.4e}       |       {4:.4e}       |       {5:.4e}       ".format(spacing, i+1, times[0, i], times[1, i], times[2, i], times[3, i]))
        print("{}---------------------------------------------------------------------------------------------------------------".format(spacing))
        print("")

    def send_hdf5(self, f):

        sh = f.shape

        num_rows = sh[0]
        num_cols = sh[1]

        mh = self.get_array_handle(f)

        chunk = 1000

        for i in range(0, num_rows, chunk):
            self.workers.send_blocks(mh, np.float64(f[i:min(num_rows, i+chunk), :]), i)

        return mh

    def get_array_handle(self, data=[], name="", sparse=0, layout=0):
        # print("Sending matrix info to Alchemist ... ", end="", flush=True)
        # start = time.time()
        (num_rows, num_cols) = data.shape

        ah = self.driver.send_array_info(name, num_rows, num_cols, sparse, layout)
        # end = time.time()
        # print("done ({0:.4e})".format(end - start))
        return ah

    def load_library(self, name, path=""):
        if self.workers_connected:
            lib_id = self.driver.load_library(name, path)
            if lib_id <= 0:
                print("ERROR: Unable to load library \'{name}\' at {path}, check path.".format(name=name, path=path))
                return 0
            else:
                module = importlib.import_module("alchemist.lib." + name + "." + name)
                library = getattr(module, name)()

                # msg = 'The {module_name} module has the following methods: {methods}'
                # print(msg.format(module_name=name, methods=dir(library)))

                library.set_id(lib_id)
                library.set_alchemist_session(self)

                self.libraries[lib_id] = library

                print("Library \'{name}\' at {path} successfully loaded.".format(name=name, path=path))
                return library

    def run_task(self, lib_id, name, in_args):
        print("Alchemist started task '" + name + "' ... ", end="", flush=True)
        start = time.time()
        out_args = self.driver.run_task(lib_id, name, in_args)
        end = time.time()
        print("done ({0:.4e}s)".format(end - start))
        return out_args

    def display_parameters(self, parameters, preamble="", spacing="    "):

        if len(preamble) > 0:
            print(preamble)
        for key, value in parameters.items():
            print(spacing + key + " = " + str(value.value) + " (" + value.datatype + ")")

    def connect_to_alchemist(self, address, port):
        self.driver.address = address
        self.driver.port = port

        self.driver.connect()

    def request_workers(self, num_requested_workers):
        # if num_requested_workers < 2:
        #     print("You can ask for more than that!")
        # elif num_requested_workers > self.driver.get_max_alchemist_workers():
        #     print("You demand too much!")
        #     print("There are just " + str(self.driver.get_max_alchemist_workers()) + " Alchemist workers in total")
        # else:
        self.workers.add_workers(self.driver.request_workers(num_requested_workers))
        self.workers.print()
        self.workers_connected = self.workers.connect()

    def send_test_string(self):
        self.driver.send_test_string()

    def request_test_string(self):
        self.driver.request_test_string()

    def list_available_libraries(self):
        self.driver.list_available_libraries()

    def convert_hdf5_to_parquet(self, h5_file, parquet_file, chunksize=100000):

        stream = pd.read_hdf(h5_file, chunksize=chunksize)

        for i, chunk in enumerate(stream):
            print("Chunk {}".format(i))

            if i == 0:
                # Infer schema and open parquet file on first chunk
                parquet_schema = pa.Table.from_pandas(df=chunk).schema
                parquet_writer = pq.ParquetWriter(parquet_file, parquet_schema, compression='snappy')

            table = pa.Table.from_pandas(chunk, schema=parquet_schema)
            parquet_writer.write_table(table)

        parquet_writer.close()

    def load_from_hdf5(self, file_name, dataset_name):
        return self.driver.load_from_hdf5(file_name, dataset_name)

    def yield_workers(self, yielded_workers=[]):
        deallocated_workers = sorted(self.driver.yield_workers(yielded_workers))

        if len(deallocated_workers) == 0:
            print("No workers were deallocated\n")
        else:
            workers_list = ""
            for i in deallocated_workers:
                workers_list += str(i)
                if i < deallocated_workers[-1]:
                    workers_list += ", "
            print("Deallocated workers " + workers_list + "\n")

    def get_matrix_info(self):
        self.driver.get_matrix_info()

    def list_alchemist_workers(self):
        return self.driver.list_all_workers()

    def list_all_workers(self, preamble=""):
        return self.driver.list_all_workers(preamble)

    def list_active_workers(self, preamble=""):
        return self.driver.list_active_workers(preamble)

    def list_inactive_workers(self, preamble=""):
        return self.driver.list_inactive_workers(preamble)

    def list_assigned_workers(self, preamble=""):
        return self.driver.list_assigned_workers(preamble)

    def stop(self):
        self.close()

    def close(self):
        self.driver.close()
        self.workers.close()



