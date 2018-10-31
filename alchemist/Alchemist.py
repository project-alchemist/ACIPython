from .DriverClient import DriverClient
from .WorkerClient import WorkerClients
import h5py
import time
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


class AlchemistSession:

    driver = []
    workers = []

    workers_connected = False

    def __init__(self):
        print("Starting Alchemist session")
        self.driver = DriverClient()
        self.workers = WorkerClients()
        self.workers_connected = False
        print("Alchemist session ready")

    def __del__(self):
        print("Ending Alchemist session")
        self.close()

    def read_from_hdf5(self, filename):
        print("Loading " + filename)
        return h5py.File(filename, 'r')

    def send_array(self, data):
        max_block_rows = 100
        max_block_cols = 20000

        (num_rows, num_cols) = data.shape

        mh = self.get_matrix_handle(data)

        print(mh.row_layout)

        self.workers.send_blocks(mh, data)
        #     self.driver.send_block(mh, block)

        return mh

    def send_hdf5(self, f):

        sh = f.shape

        num_rows = sh[0]
        num_cols = sh[1]

        mh = self.get_matrix_handle(f)

        chunk = 1000

        for i in range(0, num_rows, chunk):
            self.workers.send_blocks(mh, np.float64(f[i:min(num_rows, i+chunk), :]), i)

        return mh

    def get_array(self, mh, rows=[-1], cols=[-1]):

        if rows[0] == -1:
            num_rows = mh.num_rows
            rows = range(0, num_rows)
        else:
            num_rows = len(rows)

        if cols[0] == -1:
            num_cols = mh.num_cols
            cols = range(0, mh.num_cols)
        else:
            num_cols = len(cols)

        data = np.zeros((num_rows, num_cols))

        self.workers.get_blocks(mh, data, rows, cols)

        return data

    def get_matrix_handle(self, data):
        (num_rows, num_cols) = data.shape

        return self.driver.send_matrix_info(num_rows, num_cols)

    def load_library(self, name):
        self.driver.load_library(name)

    def run_task(self, lh, name, mh, rank):
        print("Alchemist started task " + name + " - please wait ...")
        start = time.time()
        out = self.driver.truncated_svd(lh, name, mh, rank)
        end = time.time()
        print("Elapsed time for truncated SVD is " + str(end - start))
        return out

    def connect_to_alchemist(self, address, port):
        self.driver.address = address
        self.driver.port = port

        self.driver.connect()

    def request_workers(self, num_requested_workers):
        if num_requested_workers < 2:
            print("You can ask for more than that!")
        elif num_requested_workers > self.driver.get_max_alchemist_workers():
            print("You demand too much!")
            print("There are just " + str(self.driver.get_max_alchemist_workers()) + " Alchemist workers in total")
        else:
            self.workers.set_workers(self.driver.request_workers(num_requested_workers))
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

    def load_library(self, lib_name):
        if self.workers_connected:
            lib = self.driver.load_library(lib_name)
            print("Library " + lib_name + " loaded")
            return lib
        else:
            print("Connect to Alchemist workers first")
            return []

    def load_from_hdf5(self, file_name, dataset_name):
        return self.driver.load_from_hdf5(file_name, dataset_name)

    def yield_workers(self):
        self.driver.yield_workers()

    def get_matrix_info(self):
        self.driver.get_matrix_info()

    def list_alchemist_workers(self):
        list_of_all_alchemist_workers = self.driver.list_all_alchemist_workers()

        print(list_of_all_alchemist_workers)

    def list_all_alchemist_workers(self):
        self.driver.list_all_alchemist_workers()

    def list_active_alchemist_workers(self):
        self.driver.list_active_alchemist_workers()

    def list_inactive_alchemist_workers(self):
        self.driver.list_inactive_alchemist_workers()

    def list_assigned_alchemist_workers(self):
        self.driver.list_assigned_alchemist_workers()

    def stop(self):
        self.close()

    def close(self):
        self.driver.close()
        self.workers.close()

