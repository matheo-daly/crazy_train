from google.cloud import bigquery
import google.auth
import socket
import logging
import os
from typing import Union

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


def get_bigquery_connector(key_file_location: str = None) -> Union[bigquery.Client, None]:
    f"""
    Get a big query client instantiated.

    :param key_file_location: a string that contain the Google Cloud Platform API key location. 
                              If set to None, it would take the one contained in the environment 
                              variable GOOGLE_APPLICATION_CREDENTIALS.

    :return:    a big query client object already connected to the database or None if no key file location were filled 
                neither in the configuration file thanks to the {key_file_location} or in the environment variable 
                GOOGLE_APPLICATION_CREDENTIALS

    :raises     google.auth.exceptions.DefaultCredentialsError: when the environment variable 
                GOOGLE_APPLICATION_CREDENTIALS or the bigquery configuration has no value
    """
    socket.setdefaulttimeout(600)
    try:
        return bigquery.Client() if os.environ.get(
            'GOOGLE_APPLICATION_CREDENTIALS') else bigquery.Client.from_service_account_json(key_file_location)
    except (google.auth.exceptions.DefaultCredentialsError, TypeError):
        logger.error("no GCP credentials specified in GOOGLE_APPLICATION_CREDENTIALS or config file")
        return None
