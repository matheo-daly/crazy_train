from crazy_train.connecting.redshift_connection_getter import get_redshift_connection
from crazy_train.connecting.bigquery_connection_getter import get_bigquery_connector
from crazy_train.data_retrieving.redshift_data_retriever import get_redshift_data
from crazy_train.data_writing.bigquery_writer import write_to_bigquery
from pandas import DataFrame
from google.cloud.bigquery import Client
from redshift_connector.core import Connection
from typing import Union


def transferring_data_from_redshift_to_bigquery(
        redshift_host: Union[str, None],
        redshift_database: Union[str, None],
        redshift_user: Union[str, None],
        redshift_password: Union[str, None],
        bigquery_key_file_location: Union[str, None],
        redshift_table: Union[str, None],
        bigquery_full_table_path: Union[str, None]) -> None:
    redshift_connection: Connection = get_redshift_connection(host=redshift_host,
                                                              database=redshift_database,
                                                              user=redshift_user,
                                                              password=redshift_password)
    bigquery_connection: Client = get_bigquery_connector(bigquery_key_file_location)
    redshift_data: DataFrame = get_redshift_data(conn=redshift_connection,
                                                 table=redshift_table)
    write_to_bigquery(client=bigquery_connection,
                      dataframe=redshift_data,
                      full_table_path=bigquery_full_table_path)
    redshift_connection.close()
    bigquery_connection.close()

