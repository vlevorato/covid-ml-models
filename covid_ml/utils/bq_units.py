import os
import numpy as np
from google.cloud import bigquery

from dsbox.operators.data_unit import DataInputUnit, DataOutputUnit

from covid_ml.utils.bq_client import get_bq_client, get_bq_storage_client


class JobError(Exception):
    pass


class DataInputBigQueryUnit(DataInputUnit):
    def __init__(self, query, path_json_key=None, **kwargs):
        self.kwargs_read = kwargs
        self.query = query
        self.path_json_key = path_json_key

    def read_data(self):
        if self.path_json_key is not None:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.path_json_key
        bqclient = get_bq_client()
        dataframe = bqclient.query(self.query).to_dataframe(bqstorage_client=get_bq_storage_client())
        return dataframe

    def __str__(self):
        return self.query


class DataOutputBigQueryUnit(DataOutputUnit):
    def __init__(self, table_id, drop_table=True):
        self.table_id = table_id
        self.drop_table = drop_table

    def write_data(self, dataframe):
        if len(dataframe) > 0:
            str_cols = []
            for col in dataframe.columns:
                if dataframe[col].dtype == np.object:
                    print('col str type {}'.format(col))
                    str_cols.append(bigquery.SchemaField(col, "STRING"))

            job_config = bigquery.LoadJobConfig(schema=str_cols)
            if self.drop_table:
                job_config.write_disposition = "WRITE_TRUNCATE"

            bqclient = get_bq_client()
            try:
                job = bqclient.load_table_from_dataframe(dataframe, self.table_id, job_config=job_config)
                result = job.result()
                if result.state != 'DONE':
                    raise JobError()
            except JobError:
                print('BQ job error')
                print('Job state: {}'.format(result.state))
                print('Job errors: {}'.format(result.errors))
        else:
            print("Empty dataframe, no data to write to BQ table")

    def __str__(self):
        return self.table_id