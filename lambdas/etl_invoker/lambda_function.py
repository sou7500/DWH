"""
Service: etl_invoker
Module: lambda_function
Author: Sourav Hazra
"""
import json
import os

from botocore.client import Config
import boto3

from services.etl_invoker import ETLInvokerService
from aws_lambda_powertools import Logger

session = boto3.session.Session()
config = Config(connect_timeout=5, read_timeout=5)
s3 = session.resource('s3')
logger = Logger(service="ETLInvoker")


def lambda_handler(event, context):
    """
    Frame the input for Step Functions
    :param event: Dict
    :param context: Dict
    :return: Dict
    """

    try:
        logger.append_keys(database_name="")
        logger.append_keys(table_name="")

        logger.info("Migration pipeline has been invoked")

        etl_invoker = ETLInvokerService(
            s3={
                "resource": s3,
                "bucket_name": os.getenv("SCHEMA_BUCKET_NAME"),
                "database_prefix": os.getenv("DATABASE_SCHEMA_PATH")
            },
            environment=os.getenv("ENVIRONMENT"),
            logger=logger
        )

        # Frame input for Step Functions
        response = etl_invoker.generate_input_for_step_functions()

        if response == -1:
            logger.error("Error in getting input for step functions")
            return {
                'statusCode': 500,
                'message': json.dumps('Error in getting input for step functions')
            }

        return response
    except Exception as exception:
        logger.exception(f"Exception encountered in lambda function: {exception}")
        return {
            "statusCode": 500,
            "message": "Exception encountered in lambda function"
        }
    
    