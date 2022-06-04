import redshift_connector
from redshift_connector.core import Connection
from typing import Union
import os
import logging

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


def get_redshift_connection(host: Union[str, None],
                            database: Union[str, None],
                            user: Union[str, None],
                            password: Union[str, None]) -> Union[Connection, None]:
    f"""
    get a redshift connector instantiated

    :param host:        name of the host you want to connect with
    :param user:        name of the user that has access to the database you want to retrieve information
    :param password:    password associated for the given user to connect to the redshift database
    :param database:    name of the redshift database you want to retrieve information

    :return:            a Connection Object that connect to a redshift database , 
                        or None if some connection parameters are incorrect 

    :raise:             ValueError: when a parameter for the connection is set to None and no environment 
                        variable is given to replace it. 
    """
    try:
        logger.info("establishing redshift connection")
        _host: str = os.environ.get('REDSHIFT_HOST') if os.environ.get('REDSHIFT_HOST') else host
        _database: str = os.environ.get('REDSHIFT_DATABASE') if os.environ.get('REDSHIFT_DATABASE') else database
        _user: str = os.environ.get('REDSHIFT_USER') if os.environ.get('REDSHIFT_USER') else user
        _password: str = os.environ.get('REDSHIFT_PASSWORD') if os.environ.get('REDSHIFT_PASSWORD') else password
        conn = redshift_connector.connect(
            host=_host,
            database=_database,
            user=_user,
            password=_password
        )
    except ValueError:
        logger.error("one of the redshift connection parameter is not well established")
        return None
    logger.info("redshift connection established successfully !")
    return conn
