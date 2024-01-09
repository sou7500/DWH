"""
Service: roll_back_checkpoint
Module: lambda_function
Author: Sourav Hazra
"""
import json
import os

from aws_lambda_powertools import Logger
from botocore.client import Config
import boto3

from services.rollback_service import RollBackService

# Initialize AWS service connections
session = boto3.session.Session()
config = Config(connect_timeout=5, read_timeout=5)
dynamodb = session.resource('dynamodb')
logger = Logger(service="RollBackCheckPoint")


def lambda_handler(event, context):
    """
    Lambda event handler to rollback checkpoint in DynamoDB in case of any failure
    :param event:
    :param context:
    :return: Dict
    """
    try:
        # Get the input from the Lambda event
        database_name = event.get("input").get("databaseName")
        table_name = event.get("input").get("tableName")

        logger.append_keys(database_name=database_name)
        logger.append_keys(table_name=table_name)

        # Initialize RedshiftService
        rollback_service = RollBackService(dynamodb=dynamodb, logger=logger)

        response = rollback_service.rollback_checkpoint(
            checkpoint_table_name=os.getenv("CHECKPOINT_TABLE_NAME"),
            database_name=database_name,
            table_name=table_name
        )

        if response == -1:
            return {
                'statusCode': 500,
                'message': json.dumps('Error in rolling back checkpoint')
            }

        return {
            'statusCode': 200,
            'message': "SUCCESS"
        }
    except Exception as exception:
        logger.exception(f"Exception encountered in lambda function: {exception}")
        return {
            "statusCode": 500,
            "message": "Exception encountered in lambda function"
        }
    
    