"""
Service: incremental_load
Module: lambda_function
Author: Sourav Hazra
"""
import json
import os

from aws_lambda_powertools import Logger
from botocore.client import Config
import boto3

from services.redshift_service import RedshiftService

session = boto3.session.Session()
config = Config(connect_timeout=5, read_timeout=5)
client_redshift = session.client("redshift-data", config=config)
s3 = session.resource('s3')
logger = Logger(service="IncrementalLoad")


def lambda_handler(event, context):
    """
    Lambda event handler to load data from staging table to the main table in Redshift
    :param event:
    :param context:
    :return: Dict
    """
    try:
        database_name = event.get("input").get("databaseName")
        table_name = event.get("input").get("tableName")
        staging_table_name = event.get("input").get("stagingTableName")
        redshift_database_name = event.get("input").get("redshiftDatabaseName")

        logger.append_keys(database_name=database_name)
        logger.append_keys(table_name=table_name)

        redshift = RedshiftService(
            redshift=client_redshift,
            s3={
                "resource": s3,
                "bucket_name": os.getenv("S3_BUCKET_NAME"),
                "s3_key_name": os.getenv('SQL_PATH'),
                "s3_schema_key": f"{os.getenv('TABLE_SCHEMA_PATH')}/{database_name}/{table_name}.json"
            },
            redshift_params={
                "database_name": redshift_database_name,
                "cluster_identifier": os.getenv("CLUSTER_IDENTIFIER"),
                "cluster_credentials_secret": os.getenv("CLUSTER_CREDENTIALS")
            },
            logger=logger

        )

        logger.info("Creating CreateTable stored procedure")
        response = redshift.create_incremental_load_procedure()

        if response == -1:
            logger.error("Error in reading stored procedure for incremental load")
            return {
                'statusCode': 404,
                'message': json.dumps('Error in reading stored procedure for incremental load')
            }
        if response == -2:
            logger.error("Error in creating stored procedure for incremental load")
            return {
                'statusCode': 500,
                'message': json.dumps('Error in creating stored procedure for incremental load')
            }

        logger.info("Executing stored procedure for incremental load")
        response = redshift.execute_incremental_load_stored_procedure(
            proc_name=response,
            schema=database_name,
            table=table_name,
            staging_table=staging_table_name
        )

        if response == -1:
            logger.error("Error in fetching schema")
            return {
                'statusCode': 404,
                'message': json.dumps('Error in fetching schema')
            }
        if response == -2:
            logger.error("No primary key found")
            return {
                'statusCode': 404,
                'message': json.dumps('No primary key found')
            }
        if response == -3:
            logger.error("Error in executing SQL query")
            return {
                'statusCode': 500,
                'message': json.dumps('Error in executing SQL query')
            }

        logger.info("Stored procedure for incremental load executed successfully")
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
    
    