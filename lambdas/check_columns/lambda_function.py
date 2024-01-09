"""
Service: check_columns
Module: lambda_function
Author: Sourav Hazra
"""
import json
import os

from aws_lambda_powertools import Logger
from botocore.client import Config
import boto3

from services.redshift_service import RedshiftService

# Initialize AWS service connections
session = boto3.session.Session()
config = Config(connect_timeout=5, read_timeout=5)
client_redshift = session.client("redshift-data", config=config)
s3 = session.resource('s3')
logger = Logger(service="CheckColumns")


def lambda_handler(event, context):
    """
    Lambda event handler to get table schema from S3 and make main table and staging table
    consistent with the schema
    :param event:
    :param context:
    :return: Dict
    """
    try:
        # Get the input from the Lambda event
        database_name = event.get("input").get("databaseName")
        table_name = event.get("input").get("tableName")
        staging_table_name = event.get("input").get("stagingTableName")
        redshift_database_name = event.get("input").get("redshiftDatabaseName")

        logger.append_keys(database_name=database_name)
        logger.append_keys(table_name=table_name)

        # Initialize RedshiftService
        redshift = RedshiftService(
            redshift=client_redshift,
            s3={
                "resource": s3,
                "bucket_name": os.getenv("S3_BUCKET_NAME"),
                "s3_schema_key": f"{os.getenv('TABLE_SCHEMA_PATH')}/{database_name}/{table_name}.json"
            },
            redshift_params={
                "database_name": redshift_database_name,
                "cluster_identifier": os.getenv("CLUSTER_IDENTIFIER"),
                "cluster_credentials_secret": os.getenv("CLUSTER_CREDENTIALS")
            },
            logger=logger

        )

        # Get the table schema from config file
        logger.info("Getting table schema from config file")
        schema = redshift.get_table_schema_from_definition()

        if schema == -1:
            return {
                'statusCode': 500,
                'message': json.dumps('Error in fetching schema for table')
            }

        # Make the main table consistent with schema
        logger.info("Making main table consistent with schema")
        response = redshift.make_table_consistent_with_definition(
            database_name=redshift_database_name,
            schema_name=database_name,
            table_name=table_name,
            schema=schema
        )

        if response == -1:
            return {
                'statusCode': 500,
                'message': json.dumps('Error in getting schema from Redshift')
            }
        if response == -2:
            return {
                'statusCode': 500,
                'message': json.dumps('Error in adding/deleting columns')
            }

        # Make the staging table consistent with schema
        logger.info("Making staging table consistent with schema")
        response = redshift.make_table_consistent_with_definition(
            database_name=redshift_database_name,
            schema_name=database_name,
            table_name=staging_table_name,
            schema=schema
        )

        if response == -1:
            return {
                'statusCode': 500,
                'message': json.dumps('Error in getting schema from Redshift')
            }
        if response == -2:
            return {
                'statusCode': 500,
                'message': json.dumps('Error in adding/deleting columns')
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
    
    