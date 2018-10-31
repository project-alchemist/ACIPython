from .DriverClient import DriverClient
from .WorkerClient import WorkerClients
import h5py
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


class AlchemistSession:

    driver = []
    workers = []

    def __init__(self):
        self.driver = DriverClient()
        self.workers = WorkerClients()

    def __del__(self):
        print("Ending Alchemist session")
        self.close()

    def read_from_hdf5(self, filename):
        return h5py.File(filename, 'r')

    def send_array(self, data):
        max_block_rows = 100
        max_block_cols = 10000

        (num_rows, num_cols) = data.shape

        mh = self.get_matrix_handle(data)

        print(mh.row_layout)

        self.workers.send_blocks(mh, data)
        # for i in range(0, num_rows, max_block_rows):
        #     block = data[i, min(num_rows,i+max_block_rows)]
        #     self.driver.send_block(mh, block)

        return mh

    def get_array(self, mh, row_range=[-1], col_range=[-1]):

        if row_range[0] == -1:
            num_rows = mh.num_rows
            row_range = range(0, num_rows)
        else:
            num_rows = len(row_range)

        if col_range[0] == -1:
            num_cols = mh.num_cols
            col_range = range(0, num_cols)
        else:
            num_cols = len(col_range)

        data = np.zeros((num_rows, num_cols))

        self.workers.get_blocks(mh, data, row_range, col_range)

        return data

    def get_matrix_handle(self, data):
        (num_rows, num_cols) = data.shape

        print(num_rows)
        print(num_cols)

        return self.driver.send_matrix_info(num_rows, num_cols)

    def load_library(self, name):
        self.driver.load_library(name)

    def run_task(self, name, mh, rank):
        return self.driver.truncated_svd(name, mh, rank)

    def connect_to_alchemist(self, address, port):
        self.driver.address = address
        self.driver.port = port

        return self.driver.connect()

    def request_workers(self, num_requested_workers):
        self.workers.set_workers(self.driver.request_workers(num_requested_workers))
        self.workers.print()

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
        return self.driver.load_library(lib_name)

    def load_from_hdf5(self, file_name, dataset_name):
        return self.driver.load_from_hdf5(file_name, dataset_name)

    def yield_workers(self):
        self.driver.yield_workers()

    def get_matrix_info(self):
        self.driver.get_matrix_info()

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

