import json
import unittest
from unittest.mock import patch
import boto3
from aws_lambda_powertools import Logger
from moto import mock_s3

from lambdas.incremental_load.helpers.s3_helper import S3Helper
from lambdas.incremental_load.services.redshift_service import RedshiftService
from lambdas.incremental_load.lambda_function import lambda_handler

logger = Logger()


class TestIncrementalLoad(unittest.TestCase):
    @mock_s3
    def test_helper_fetch_object_without_exception(self):
        s3 = boto3.resource('s3')
        bucket = s3.Bucket('sample_bucket')
        bucket.create(CreateBucketConfiguration={
            'LocationConstraint': 'ap-south-1',
        })
        s3.Object("sample_bucket", "object_name.txt").put(
            Body="Hello, World!"
        )
        s3_helper = S3Helper(logger=logger)
        assert s3_helper.fetch_object(s3, bucket_name='sample_bucket', key='object_name.txt') == 'Hello, World!'

    @mock_s3
    def test_helper_fetch_object_with_exception(self):
        s3 = boto3.resource('s3')
        bucket = s3.Bucket('sample_bucket')
        bucket.create(CreateBucketConfiguration={
            'LocationConstraint': 'ap-south-1',
        })
        s3.Object("sample_bucket", "object_name.txt").put(
            Body="Hello, World!"
        )
        s3_helper = S3Helper(logger=logger)
        assert s3_helper.fetch_object(s3, bucket_name='sample_bucket1', key='object_name.txt') is None

    @patch('lambdas.create_table.services.redshift_service.S3Helper.fetch_object')
    def test_create_incremental_load_procedure_fetch_unsuccessful(self, fetch_object):
        fetch_object.return_value = None
        redshift = RedshiftService(
            redshift=None,
            s3={},
            redshift_params={},
            logger=logger
        )
        assert redshift.create_incremental_load_procedure() == -1

    @patch('lambdas.create_table.services.redshift_service.S3Helper.fetch_object')
    def test_create_incremental_load_procedure_proc_name_error(self, fetch_object):
        fetch_object.return_value = "CREATE OR REPLACE PROCEDURE procedure"
        redshift = RedshiftService(
            redshift=None,
            s3={},
            redshift_params={},
            logger=logger
        )
        assert redshift.create_incremental_load_procedure() == -3

    @patch('lambdas.create_table.services.redshift_service.S3Helper.fetch_object')
    @patch('lambdas.create_table.services.redshift_service.RedshiftHelper.run_query')
    def test_create_incremental_load_procedure_create_unsuccessful(self, run_query, fetch_object):
        run_query.return_value = None
        fetch_object.return_value = "CREATE OR REPLACE PROCEDURE procedure(something)"
        redshift = RedshiftService(
            redshift=None,
            s3={},
            redshift_params={},
            logger=logger
        )
        assert redshift.create_incremental_load_procedure() == -2

    @patch('lambdas.create_table.services.redshift_service.S3Helper.fetch_object')
    @patch('lambdas.create_table.services.redshift_service.RedshiftHelper.run_query')
    def test_create_incremental_load_procedure_create_successful(self, run_query, fetch_object):
        run_query.return_value = True
        fetch_object.return_value = "CREATE OR REPLACE PROCEDURE procedure(something)"
        redshift = RedshiftService(
            redshift=None,
            s3={},
            redshift_params={},
            logger=logger
        )
        assert redshift.create_incremental_load_procedure() == "procedure"

    @patch('lambdas.create_table.services.redshift_service.S3Helper.fetch_object')
    def test_execute_incremental_load_stored_procedure_unsuccessful_fetch(self, fetch_object):
        fetch_object.return_value = None
        redshift = RedshiftService(
            redshift=None,
            s3={},
            redshift_params={},
            logger=logger
        )
        assert redshift.execute_incremental_load_stored_procedure() == -1

    @patch('lambdas.create_table.services.redshift_service.S3Helper.fetch_object')
    def test_execute_incremental_load_stored_procedure_primary_key_missing(self, fetch_object):
        fetch_object.return_value = json.dumps(
            {
                "tableConfigurations": {
                    "some": "value"
                }
            }
        )
        redshift = RedshiftService(
            redshift=None,
            s3={},
            redshift_params={},
            logger=logger
        )
        assert redshift.execute_incremental_load_stored_procedure() == -2

    @patch('lambdas.create_table.services.redshift_service.RedshiftHelper.run_query')
    @patch('lambdas.create_table.services.redshift_service.S3Helper.fetch_object')
    def test_execute_incremental_load_stored_procedure_query_unsuccessful(self, fetch_object, run_query):
        fetch_object.return_value = json.dumps(
            {
                "tableConfigurations": {
                    "primaryKey": "id"
                }
            }
        )
        run_query.return_value = None
        redshift = RedshiftService(
            redshift=None,
            s3={},
            redshift_params={},
            logger=logger
        )
        assert redshift.execute_incremental_load_stored_procedure() == -4

    @patch('lambdas.create_table.services.redshift_service.RedshiftHelper.run_query')
    @patch('lambdas.create_table.services.redshift_service.S3Helper.fetch_object')
    def test_execute_incremental_load_stored_procedure_query_successful(self, fetch_object, run_query):
        fetch_object.return_value = json.dumps(
            {
                "tableConfigurations": {
                    "primaryKey": "id"
                }
            }
        )
        run_query.return_value = True
        redshift = RedshiftService(
            redshift=None,
            s3={},
            redshift_params={},
            logger=logger
        )
        assert redshift.execute_incremental_load_stored_procedure() is True

    def test_lambda_handler_no_input(self):
        expected_output = {
            "statusCode": 500,
            "message": "Exception encountered in lambda function"
        }
        assert lambda_handler(event={}, context=None) == expected_output

    @patch('lambdas.create_table.lambda_function.RedshiftService.create_incremental_load_procedure')
    def test_lambda_handler_stored_proc_fetch_unsuccessful(self, create_incremental_load_procedure):
        create_incremental_load_procedure.return_value = -1
        expected_output = {
            'statusCode': 404,
            'message': json.dumps('Error in reading stored procedure for incremental load')
        }
        assert lambda_handler(event={"input": {}}, context=None) == expected_output

    @patch('lambdas.create_table.lambda_function.RedshiftService.create_incremental_load_procedure')
    def test_lambda_handler_stored_proc_execution_unsuccessful(self, create_incremental_load_procedure):
        create_incremental_load_procedure.return_value = -2
        expected_output = {
            'statusCode': 500,
            'message': json.dumps('Error in creating stored procedure for incremental load')
        }
        assert lambda_handler(event={"input": {}}, context=None) == expected_output

    @patch('lambdas.create_table.lambda_function.RedshiftService.execute_incremental_load_stored_procedure')
    @patch('lambdas.create_table.lambda_function.RedshiftService.create_incremental_load_procedure')
    def test_lambda_handler_schema_fetch_unsuccessful(self, create_incremental_load_procedure,
                                                      execute_incremental_load_stored_procedure):
        create_incremental_load_procedure.return_value = True
        execute_incremental_load_stored_procedure.return_value = -1
        expected_output = {
            'statusCode': 404,
            'message': json.dumps('Error in fetching schema')
        }
        assert lambda_handler(event={"input": {}}, context=None) == expected_output

    @patch('lambdas.create_table.lambda_function.RedshiftService.execute_incremental_load_stored_procedure')
    @patch('lambdas.create_table.lambda_function.RedshiftService.create_incremental_load_procedure')
    def test_lambda_handler_primary_key_missing(self, create_incremental_load_procedure,
                                                execute_incremental_load_stored_procedure):
        create_incremental_load_procedure.return_value = True
        execute_incremental_load_stored_procedure.return_value = -2
        expected_output = {
            'statusCode': 404,
            'message': json.dumps('No primary key found')
        }
        assert lambda_handler(event={"input": {}}, context=None) == expected_output

    @patch('lambdas.create_table.lambda_function.RedshiftService.execute_incremental_load_stored_procedure')
    @patch('lambdas.create_table.lambda_function.RedshiftService.create_incremental_load_procedure')
    def test_lambda_handler_stored_procedure_execution_unsuccessful(self, create_incremental_load_procedure,
                                                                    execute_incremental_load_stored_procedure):
        create_incremental_load_procedure.return_value = True
        execute_incremental_load_stored_procedure.return_value = -4
        expected_output = {
            'statusCode': 500,
            'message': json.dumps('Error in executing SQL query')
        }
        assert lambda_handler(event={"input": {}}, context=None) == expected_output

    @patch('lambdas.create_table.lambda_function.RedshiftService.execute_incremental_load_stored_procedure')
    @patch('lambdas.create_table.lambda_function.RedshiftService.create_incremental_load_procedure')
    def test_lambda_handler_successful(self, create_incremental_load_procedure,
                                       execute_incremental_load_stored_procedure):
        create_incremental_load_procedure.return_value = True
        execute_incremental_load_stored_procedure.return_value = True
        expected_output = {
            'statusCode': 200,
            'message': "SUCCESS"
        }
        assert lambda_handler(event={"input": {}}, context=None) == expected_output
