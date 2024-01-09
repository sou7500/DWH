"""
Service: execute_sql
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
logger = Logger(service="ExecuteSQL")


def lambda_handler(event, context):
    """
    Lambda event handler to execute any SQL statement, stored procedure and sql file
    :param event:
    :param context:
    :return: Dict
    """
    try:
        # Get the input from the Lambda event
        database_name = event.get("input").get("databaseName")
        sql_statement_key = event.get("input").get("sqlStatementKey")
        sql_statement_type = event.get("input").get("sqlStatementType")
        proc_arguments = event.get("input").get("procArguments")
        sql_query = event.get("input").get("sqlQuery")

        logger.append_keys(database_name=database_name)
        logger.append_keys(table_name="")

        if sql_statement_type == "PROC" and not sql_statement_key:
            logger.error("Stored Procedure key name not found")
            return {
                'statusCode': 404,
                'message': json.dumps('Stored Procedure key name not found')
            }
        if sql_statement_type == "ADHOC" and not sql_query:
            logger.error("SQL query not found")
            return {
                'statusCode': 404,
                'message': json.dumps('SQL query not found')
            }
        if sql_statement_type not in ("PROC", "ADHOC") and not sql_statement_key:
            logger.error("SQL query key name not found")
            return {
                'statusCode': 404,
                'message': json.dumps('SQL query key name not found')
            }
        if sql_statement_key and sql_statement_key.split("/")[-1].split(".")[-1] != "sql":
            logger.error("Not a S3 object")
            return {
                'statusCode': 200,
                'message': json.dumps('Not a S3 object')
            }

        # Initialize RedshiftService
        redshift = RedshiftService(
            redshift=client_redshift,
            s3={
                "resource": s3,
                "bucket_name": os.getenv("S3_BUCKET_NAME"),
                "s3_sql_key": sql_statement_key,
            },
            redshift_params={
                "database_name": database_name,
                "cluster_identifier": os.getenv("CLUSTER_IDENTIFIER"),
                "cluster_credentials_secret": os.getenv("CLUSTER_CREDENTIALS")
            },
            logger=logger

        )

        logger.info(f"Running SQL statement: Type={sql_statement_type}, Query={sql_query}, Args={proc_arguments}")

        # Dynamically frame the create table stored procedure based on the main table name, schema name, staging table
        # name and database name
        response = redshift.run_sql_statement(
            type=sql_statement_type,
            proc_arguments=proc_arguments,
            query=sql_query
        )

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

        logger.info("SQL query executed successfully")

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
    
    