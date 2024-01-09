import json
import unittest
from unittest.mock import patch
import boto3
from aws_lambda_powertools import Logger
from moto import mock_s3

from lambdas.create_table.helpers.s3_helper import S3Helper
from lambdas.create_table.services.redshift_service import RedshiftService
from lambdas.create_table.lambda_function import lambda_handler

logger = Logger()


class TestCreateTable(unittest.TestCase):
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
    def test_frame_create_table_stored_procedure_schema_fetch_unsuccessful(self, fetch_object):
        fetch_object.return_value = None
        redshift = RedshiftService(
            redshift=None,
            s3={},
            redshift_params={},
            logger=logger
        )
        assert redshift.frame_create_table_stored_procedure() == -1

    @patch('lambdas.create_table.services.redshift_service.S3Helper.fetch_object')
    def test_frame_create_table_stored_procedure_sp_fetch_unsuccessful(self, fetch_object):
        fetch_object.side_effect = [
            json.dumps({
                "columns": {
                    "id": "varchar",
                    "address_line_1": "varchar"
                }
            }),
            None
        ]
        redshift = RedshiftService(
            redshift=None,
            s3={},
            redshift_params={},
            logger=logger
        )
        assert redshift.frame_create_table_stored_procedure() == -1

    @patch('lambdas.create_table.services.redshift_service.S3Helper.fetch_object')
    @patch('lambdas.create_table.services.redshift_service.RedshiftHelper.run_query')
    def test_frame_create_table_stored_procedure_success_without_table_config(self, run_query, fetch_object):
        fetch_object.side_effect = [
            json.dumps({
                "columns": {
                    "id": "varchar",
                    "address_line_1": "varchar"
                }
            }),
            "CREATE OR REPLACE PROCEDURE procedure(dfvn odnfvk ldkfnfv)"
        ]
        run_query.return_value = True
        redshift = RedshiftService(
            redshift=None,
            s3={},
            redshift_params={},
            logger=logger
        )
        assert redshift.frame_create_table_stored_procedure() == "procedure"

    @patch('lambdas.create_table.services.redshift_service.S3Helper.fetch_object')
    @patch('lambdas.create_table.services.redshift_service.RedshiftHelper.run_query')
    def test_frame_create_table_stored_procedure_success_without_redshift_config(self, run_query, fetch_object):
        fetch_object.side_effect = [
            json.dumps({
                "columns": {
                    "id": "varchar",
                    "address_line_1": "varchar"
                },
                "tableConfigurations": {
                    "primaryKey": "id"
                }
            }),
            "CREATE OR REPLACE PROCEDURE procedure(dfvn odnfvk ldkfnfv)"
        ]
        run_query.return_value = True
        redshift = RedshiftService(
            redshift=None,
            s3={},
            redshift_params={},
            logger=logger
        )
        assert redshift.frame_create_table_stored_procedure() == "procedure"

    @patch('lambdas.create_table.services.redshift_service.S3Helper.fetch_object')
    @patch('lambdas.create_table.services.redshift_service.RedshiftHelper.run_query')
    def test_frame_create_table_stored_procedure_success_with_all_config(self, run_query, fetch_object):
        fetch_object.side_effect = [
            json.dumps({
                "columns": {
                    "id": "varchar",
                    "address_line_1": "varchar"
                },
                "tableConfigurations": {
                    "primaryKey": "id"
                },
                "redshiftConfigurations": {
                    "distkey": "card_type_name",
                    "compoundSortkey": "created_at"
                }
            }),
            "CREATE OR REPLACE PROCEDURE procedure(dfvn odnfvk ldkfnfv)"
        ]
        run_query.return_value = True
        redshift = RedshiftService(
            redshift=None,
            s3={},
            redshift_params={},
            logger=logger
        )
        assert redshift.frame_create_table_stored_procedure() == "procedure"

    @patch('lambdas.create_table.services.redshift_service.S3Helper.fetch_object')
    def test_frame_create_table_stored_procedure_invalid_stored_procedure(self, fetch_object):
        fetch_object.side_effect = [
            json.dumps({
                "columns": {
                    "id": "varchar",
                    "address_line_1": "varchar"
                },
                "tableConfigurations": {
                    "primaryKey": "id"
                },
                "redshiftConfigurations": {
                    "distkey": "card_type_name",
                    "compoundSortkey": "created_at"
                }
            }),
            "CREATE OR REPLACE PROCEDURE procedure"
        ]
        redshift = RedshiftService(
            redshift=None,
            s3={},
            redshift_params={},
            logger=logger
        )
        assert redshift.frame_create_table_stored_procedure() == -3

    @patch('lambdas.create_table.services.redshift_service.S3Helper.fetch_object')
    @patch('lambdas.create_table.services.redshift_service.RedshiftHelper.run_query')
    def test_frame_create_table_stored_procedure_create_proc_unsuccessful(self, run_query, fetch_object):
        fetch_object.side_effect = [
            json.dumps({
                "columns": {
                    "id": "varchar",
                    "address_line_1": "varchar"
                },
                "tableConfigurations": {
                    "primaryKey": "id"
                },
                "redshiftConfigurations": {
                    "distkey": "card_type_name",
                    "compoundSortkey": "created_at"
                }
            }),
            "CREATE OR REPLACE PROCEDURE procedure(dfvn odnfvk ldkfnfv)"
        ]
        run_query.return_value = False
        redshift = RedshiftService(
            redshift=None,
            s3={},
            redshift_params={},
            logger=logger
        )
        assert redshift.frame_create_table_stored_procedure() == -2

    @patch('lambdas.create_table.services.redshift_service.RedshiftHelper.run_query')
    def test_execute_create_table_stored_procedure_unsuccessful(self, run_query):
        run_query.return_value = True
        redshift = RedshiftService(
            redshift=None,
            s3={},
            redshift_params={},
            logger=logger
        )
        assert redshift.execute_create_table_stored_procedure() is True

    def test_lambda_handler_no_input(self):
        expected_output = {
            "statusCode": 500,
            "message": "Exception encountered in lambda function"
        }
        assert lambda_handler(event={}, context=None) == expected_output

    @patch('lambdas.create_table.lambda_function.RedshiftService.frame_create_table_stored_procedure')
    def test_lambda_handler_error_code_for_reading_sql(self, frame_create_table_stored_procedure):
        frame_create_table_stored_procedure.return_value = -1
        expected_output = {
            'statusCode': 404,
            'message': json.dumps('Error in reading SQL query')
        }
        assert lambda_handler(event={"input": {}}, context=None) == expected_output

    @patch('lambdas.create_table.lambda_function.RedshiftService.frame_create_table_stored_procedure')
    def test_lambda_handler_error_code_for_executing_sql(self, frame_create_table_stored_procedure):
        frame_create_table_stored_procedure.return_value = -2
        expected_output = {
            'statusCode': 500,
            'message': json.dumps('Error in executing SQL query')
        }
        assert lambda_handler(event={"input": {}}, context=None) == expected_output

    @patch('lambdas.create_table.lambda_function.RedshiftService.frame_create_table_stored_procedure')
    def test_lambda_handler_error_code_for_getting_stored_procedure(self, frame_create_table_stored_procedure):
        frame_create_table_stored_procedure.return_value = -3
        expected_output = {
            'statusCode': 500,
            'message': json.dumps('Error in getting stored procedure name')
        }
        assert lambda_handler(event={"input": {}}, context=None) == expected_output

    @patch('lambdas.create_table.lambda_function.RedshiftService.execute_create_table_stored_procedure')
    @patch('lambdas.create_table.lambda_function.RedshiftService.frame_create_table_stored_procedure')
    def test_lambda_handler_error_code_for_cretaing_table(self, frame_create_table_stored_procedure, execute_create_table_stored_procedure):
        frame_create_table_stored_procedure.return_value = True
        execute_create_table_stored_procedure.return_value = -1
        expected_output = {
            'statusCode': 500,
            'message': json.dumps('Error in executing SQL query')
        }
        assert lambda_handler(event={"input": {}}, context=None) == expected_output

    @patch('lambdas.create_table.lambda_function.RedshiftService.execute_create_table_stored_procedure')
    @patch('lambdas.create_table.lambda_function.RedshiftService.frame_create_table_stored_procedure')
    def test_lambda_handler_error_code_success(self, frame_create_table_stored_procedure, execute_create_table_stored_procedure):
        frame_create_table_stored_procedure.return_value = True
        execute_create_table_stored_procedure.return_value = True
        expected_output = {
            'statusCode': 200,
            'message': "SUCCESS"
        }
        assert lambda_handler(event={"input": {}}, context=None) == expected_output