from .Client import DriverClient, WorkerClients
from .MatrixHandle import MatrixHandle
from .Parameter import Parameter
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

    def __init__(self, driver_buffer_length = 10000, worker_buffer_length = 10000000,
                 verbose = True, show_overheads = False):
        print("Starting Alchemist session ... ", end="", flush=True)
        self.driver = DriverClient(driver_buffer_length, verbose, show_overheads)
        self.workers = WorkerClients(worker_buffer_length, verbose, show_overheads)
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

    def send_matrix(self, matrix, print_times=False, layout="MC_MR"):
        max_block_rows = 100
        max_block_cols = 20000

        (num_rows, num_cols) = matrix.shape

        print("Sending array info to Alchemist ... ", end="", flush=True)
        start = time.time()
        mh = self.get_matrix_handle(matrix, layout=layout)
        end = time.time()
        print("done ({0:.4e}s)".format(end - start))

        print("Sending array data to Alchemist ... ", end="", flush=True)
        start = time.time()
        times = self.workers.send_matrix_blocks(mh, matrix)
        end = time.time()
        print("done ({0:.4e}s)".format(end - start))
        if print_times:
            self.print_times(times, name=mh.name)
        #     self.driver.send_block(mh, block)

        return mh

    def fetch_matrix(self, mh, print_times=False):

        matrix = np.zeros((mh.num_rows, mh.num_cols))

        print("Fetching data for array {0} from Alchemist ... ".format(mh.name), end="", flush=True)
        start = time.time()
        matrix, times = self.workers.get_matrix_blocks(mh, matrix)
        end = time.time()
        print("done ({0:.4e}s)".format(end - start))
        if print_times:
            self.print_times(times, name=mh.name)
        return matrix

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
            serialization_times = times[i][0]
            send_times = times[i][1]
            receive_times = times[i][2]
            deserialization_times = times[i][3]

            print("{0}    {1:3d}   |       {2:.4e}       |       {3:.4e}       |       {4:.4e}       |       {5:.4e}       ".format(
                    spacing, i + 1, serialization_times[0], send_times[0], receive_times[0], deserialization_times[0]))
            for j in range(1, len(serialization_times)):
                print("{0}          |       {1:.4e}       |       {2:.4e}       |       {3:.4e}       |       {4:.4e}       ".format(
                    spacing, serialization_times[j], send_times[j], receive_times[j], deserialization_times[j]))
            if i < self.workers.num_workers - 1:
                print("{0}{1}".format(spacing, ' -' * 55))
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

    def get_matrix_handle(self, data=[], name="", sparse=0, layout="MC_MR"):
        # print("Sending matrix info to Alchemist ... ", end="", flush=True)
        # start = time.time()
        (num_rows, num_cols) = data.shape

        ah = self.driver.send_matrix_info(name, num_rows, num_cols, sparse, MatrixHandle.layouts[layout])
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

                print("Library \'{name}\' successfully loaded.".format(name=name, path=path))
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
        for key, p in parameters.items():
            print(spacing + p.to_string())
        # for key, value in parameters.items():
        #     dt_name = ""
        #     for name, code in Parameter.datatypes.items():
        #         if code == value.datatype:
        #             dt_name = name
        #     print(spacing + key + " = " + str(value.value) + " (" + dt_name + ")")

    def connect_to_alchemist(self, hostname, port):
        self.driver.hostname = hostname
        self.driver.port = port

        self.driver.connect()

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

    def get_matrix_info(self):
        self.driver.get_matrix_info()

    def request_workers(self, num_requested_workers):

        requested_workers = self.driver.request_workers(num_requested_workers)
        if len(requested_workers) > 0:
            self.workers.add_workers(requested_workers)
            self.workers_connected = self.workers.connect()
        else:
            print("Alchemist could not allocate workers")

        self.workers.print()

    def yield_workers(self, yielded_workers=[]):
        deallocated_workers = self.driver.yield_workers(yielded_workers)
        if len(deallocated_workers) == 0:
            print("No workers were deallocated")
        else:
            s = ""
            if len(deallocated_workers) > 1:
                s = "s"
            print("Listing {0} deallocated Alchemist worker{1}:".format(len(deallocated_workers), s))
            self.workers.print(deallocated_workers)

    def list_alchemist_workers(self):
        all_workers = self.driver.list_all_workers()
        if len(all_workers) == 0:
            print("No Alchemist workers")
        else:
            s = ""
            if len(all_workers) > 1:
                s = "s"
            print("Listing {0} Alchemist worker{1}:".format(len(all_workers), s))
            self.workers.print(all_workers)

    def list_all_workers(self):
        all_workers = self.driver.list_all_workers()
        if len(all_workers) == 0:
            print("No Alchemist workers")
        else:
            s = ""
            if len(all_workers) > 1:
                s = "s"
            print("Listing {0} Alchemist worker{1}:".format(len(all_workers), s))
            self.workers.print(all_workers)

    def list_active_workers(self):
        active_workers = self.driver.list_active_workers()
        if len(active_workers) == 0:
            print("No active Alchemist workers")
        else:
            s = ""
            if len(active_workers) > 1:
                s = "s"
            print("Listing {0} active Alchemist worker{1}:".format(len(active_workers), s))
            self.workers.print(active_workers)

    def list_inactive_workers(self):
        inactive_workers = self.driver.list_inactive_workers()
        if len(inactive_workers) == 0:
            print("No inactive Alchemist workers")
        else:
            s = ""
            if len(inactive_workers) > 1:
                s = "s"
            print("Listing {0} inactive Alchemist worker{1}:".format(len(inactive_workers), s))
            self.workers.print(inactive_workers)

    def list_assigned_workers(self):
        assigned_workers = self.driver.list_assigned_workers()
        if len(assigned_workers) == 0:
            print("No assigned Alchemist workers")
        else:
            s = ""
            if len(assigned_workers) > 1:
                s = "s"
            print("Listing {0} assigned Alchemist worker{1}:".format(len(assigned_workers), s))
            self.workers.print(assigned_workers)

    def stop(self):
        self.close()

    def close(self):
        self.driver.close()
        self.workers.close()



