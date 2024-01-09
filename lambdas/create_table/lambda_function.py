"""
Service: create_table
Module: lambda_function
Author: Sourav Hazra
"""
import json
import os

from botocore.client import Config
import boto3

from services.redshift_service import RedshiftService
from aws_lambda_powertools import Logger

# Initialize AWS service connections
session = boto3.session.Session()
config = Config(connect_timeout=5, read_timeout=5)
client_redshift = session.client("redshift-data", config=config)
s3 = session.resource('s3')
logger = Logger(service="CreateTable")


def lambda_handler(event, context):
    """
    Lambda event handler to fetch the create table stored procedure from S3 and execute it to create
    the main and staging tables
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

        logger.info("Creating table")

        # Initialize RedshiftService
        redshift = RedshiftService(
            redshift=client_redshift,
            s3={
                "resource": s3,
                "bucket_name": os.getenv("S3_BUCKET_NAME"),
                "s3_create_proc_key": os.getenv('CREATE_PROC_PATH'),
                "s3_schema_key": f"{os.getenv('TABLE_SCHEMA_PATH')}/{database_name}/{table_name}.json"
            },
            redshift_params={
                "database_name": redshift_database_name,
                "cluster_identifier": os.getenv("CLUSTER_IDENTIFIER"),
                "cluster_credentials_secret": os.getenv("CLUSTER_CREDENTIALS")
            },
            logger=logger

        )

        # Dynamically frame the create table stored procedure based on the main table name, schema name, staging table
        # name and database name
        response = redshift.frame_create_table_stored_procedure()

        if response == -1:
            logger.error("Error in reading SQL query")
            return {
                'statusCode': 404,
                'message': json.dumps('Error in reading SQL query')
            }
        if response == -2:
            logger.error("Error in executing SQL query")
            return {
                'statusCode': 500,
                'message': json.dumps('Error in executing SQL query')
            }
        if response == -3:
            logger.error("Error in getting stored procedure name")
            return {
                'statusCode': 500,
                'message': json.dumps('Error in getting stored procedure name')
            }

        logger.info("Executing CreateTable stored procedure")

        # Execute framed stored procedure
        response = redshift.execute_create_table_stored_procedure(
            proc_name=response,
            schema=database_name,
            table=table_name,
            staging_table=staging_table_name,
        )

        if response == -1:
            logger.error(f"Error in creating table {table_name}")
            return {
                'statusCode': 500,
                'message': json.dumps('Error in executing SQL query')
            }

        logger.info(f"{table_name} created successfully under database {database_name}")

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
    
    