import logging
import time

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


def lambda_handle(event, context):
    print('hello')
