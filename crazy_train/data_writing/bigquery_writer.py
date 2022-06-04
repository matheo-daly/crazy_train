from google.cloud import bigquery
import pandas as pd
import logging
import os
from typing import Union

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


def write_to_bigquery(full_table_path: Union[str, None], client: bigquery.Client, dataframe: pd.DataFrame) -> None:
    f"""
    write a dataframe into a bigquery table

    :param full_table_path: full name of the dataset and the table where data needs to be written
    :param client : bigquery client instantiated
    :param dataframe: dataframe that needs to be written to bigquery

    :return: None
    """
    _full_table_path: str = os.environ.get('BIGQUERY_FULL_TABLE_PATH') if os.environ.get('BIGQUERY_FULL_TABLE_PATH') \
        else full_table_path
    logger.info(f"writing into {_full_table_path}...")
    client.load_table_from_dataframe(dataframe, _full_table_path)
    logger.info(f"data has been written into {_full_table_path} with success !")

    return None
