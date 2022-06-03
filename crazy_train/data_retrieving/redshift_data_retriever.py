import logging
from jinjasql import JinjaSql
import pandas as pd
from redshift_connector.core import Connection

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


def get_redshift_data(conn: Connection, table: str) -> pd.DataFrame:
    f"""
    get a pandas dataframe with data retrieved from a certain redshift table
    
    :param conn:    a redshift connection instantiated and already connected to required redshift database
    :param table:   name of the table from which the data needs to be retrieved 
    
    :return:        a pandas dataframe with all data retrieved from a certain redshift table
    """
    query: str = f"""
      SELECT 
        *
      FROM
        {table}
    """

    params = {}
    j = JinjaSql()
    template = query.replace('}}', ' | sqlsafe }}')
    query = j.prepare_query(template, params)[0]

    dataframe = pd.read_sql_query(con=conn, sql=query)
    logger.info(
        f"redshift data retrieved from table {table}")
    return dataframe