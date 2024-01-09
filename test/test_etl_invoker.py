import json

import boto3
from moto import mock_s3
import unittest
from unittest.mock import patch
from freezegun import freeze_time

from aws_lambda_powertools import Logger

from lambdas.etl_invoker.helpers.s3_helper import S3Helper
from lambdas.etl_invoker.services.etl_invoker import ETLInvokerService
from lambdas.etl_invoker.lambda_function import lambda_handler

logger = Logger()


class TestETLInvoker(unittest.TestCase):

    @mock_s3
    def test_helper_list_objects_without_exception(self):
        s3 = boto3.resource('s3')
        bucket = s3.Bucket('sample_bucket')
        bucket.create(CreateBucketConfiguration={
            'LocationConstraint': 'ap-south-1',
        })
        bucket.upload_file('test_etl_invoker.py', 'a.json')
        bucket.upload_file('test_etl_invoker.py', 'b.json')
        s3_helper = S3Helper(logger=logger)

        expected_output = ["a.json", "b.json"]

        assert s3_helper.list_objects(s3, bucket_name='sample_bucket') == expected_output

    @mock_s3
    def test_helper_list_objects_with_exception(self):
        s3 = boto3.resource('s3')
        bucket = s3.Bucket('sample_bucket1')
        bucket.create(CreateBucketConfiguration={
            'LocationConstraint': 'ap-south-1',
        })
        bucket.upload_file('test_etl_invoker.py', 'a.json')
        bucket.upload_file('test_etl_invoker.py', 'b.json')
        s3_helper = S3Helper(logger=logger)

        assert s3_helper.list_objects(s3, bucket_name='sample_bucket') is None

    @patch('lambdas.etl_invoker.services.etl_invoker.S3Helper.list_objects')
    def test_get_databases_to_migrate_no_matching_prefix(self, list_objects):
        list_objects.return_value = ["a.json", "b.json"]
        etl_invoker_service = ETLInvokerService(
            s3={
                "database_prefix": "a/b/c"
            },
            environment=None,
            logger=logger
        )
        assert etl_invoker_service.get_databases_to_migrate() == []

    @patch('lambdas.etl_invoker.services.etl_invoker.S3Helper.list_objects')
    def test_get_databases_to_migrate_matching_prefix(self, list_objects):
        list_objects.return_value = ["a/b/c/d.json", "b.json"]
        etl_invoker_service = ETLInvokerService(
            s3={
                "database_prefix": "a/b/c"
            },
            environment=None,
            logger=logger
        )
        assert etl_invoker_service.get_databases_to_migrate() == ["a/b/c/d.json"]

    @patch('lambdas.etl_invoker.services.etl_invoker.S3Helper.list_objects')
    def test_get_databases_to_migrate_no_databases(self, list_objects):
        list_objects.return_value = []
        etl_invoker_service = ETLInvokerService(
            s3={
                "database_prefix": "a/b/c"
            },
            environment=None,
            logger=logger
        )
        assert etl_invoker_service.get_databases_to_migrate() == []

    @patch('lambdas.etl_invoker.services.etl_invoker.S3Helper.list_objects')
    def test_get_databases_to_migrate_error_fetch_database(self, list_objects):
        list_objects.return_value = None
        etl_invoker_service = ETLInvokerService(
            s3={
                "database_prefix": "a/b/c"
            },
            environment=None,
            logger=logger
        )
        assert etl_invoker_service.get_databases_to_migrate() == -1

    @freeze_time("2012-01-12")
    @patch('lambdas.etl_invoker.services.etl_invoker.ETLInvokerService.get_databases_to_migrate')
    def test_generate_input_for_step_functions_no_database(self, databases):
        databases.return_value = -1
        etl_invoker_service = ETLInvokerService(
            s3={},
            logger=logger
        )
        assert etl_invoker_service.generate_input_for_step_functions() == -1

    @freeze_time("2012-01-12")
    @patch('lambdas.etl_invoker.services.etl_invoker.ETLInvokerService.get_databases_to_migrate')
    def test_generate_input_for_step_functions_with_database(self, databases):
        databases.return_value = ["a/b/c.json", "a/b/d.json"]
        expected_output = [
            {
                "input": {
                    "databaseName": "c",
                    "environment": None,
                    "configBucket": None,
                    "configFileKey": "a/b/c.json",
                    "monthlyBackUp": 0
                }
            },
            {
                "input": {
                    "databaseName": "d",
                    "environment": None,
                    "configBucket": None,
                    "configFileKey": "a/b/d.json",
                    "monthlyBackUp": 0
                }
            }
        ]
        etl_invoker_service = ETLInvokerService(
            s3={},
            logger=logger
        )
        assert etl_invoker_service.generate_input_for_step_functions() == expected_output
    @freeze_time("2012-01-01")
    @patch('lambdas.etl_invoker.services.etl_invoker.ETLInvokerService.get_databases_to_migrate')
    def test_generate_input_for_step_functions_with_database_first_day(self, databases):
        databases.return_value = ["a/b/c.json", "a/b/d.json"]
        expected_output = [
            {
                "input": {
                    "databaseName": "c",
                    "environment": None,
                    "configBucket": None,
                    "configFileKey": "a/b/c.json",
                    "monthlyBackUp": 1
                }
            },
            {
                "input": {
                    "databaseName": "d",
                    "environment": None,
                    "configBucket": None,
                    "configFileKey": "a/b/d.json",
                    "monthlyBackUp": 1
                }
            }
        ]
        etl_invoker_service = ETLInvokerService(
            s3={},
            logger=logger
        )
        assert etl_invoker_service.generate_input_for_step_functions() == expected_output
    @freeze_time("2012-01-12")
    @patch('lambdas.etl_invoker.services.etl_invoker.ETLInvokerService.get_databases_to_migrate')
    def test_generate_input_for_step_functions_with_database_without_prefix(self, databases):
        databases.return_value = ["c.json", "d.json"]
        expected_output = [
            {
                "input": {
                    "databaseName": "c",
                    "environment": None,
                    "configBucket": None,
                    "configFileKey": "c.json",
                    "monthlyBackUp": 0
                }
            },
            {
                "input": {
                    "databaseName": "d",
                    "environment": None,
                    "configBucket": None,
                    "configFileKey": "d.json",
                    "monthlyBackUp": 0
                }
            }
        ]
        etl_invoker_service = ETLInvokerService(
            s3={},
            logger=logger
        )
        assert etl_invoker_service.generate_input_for_step_functions() == expected_output

    @patch('lambdas.etl_invoker.services.etl_invoker.ETLInvokerService.get_databases_to_migrate')
    def test_generate_input_for_step_functions_with_database_without_extension(self, databases):
        databases.return_value = ["c", "d"]
        etl_invoker_service = ETLInvokerService(
            s3={},
            logger=logger
        )
        assert etl_invoker_service.generate_input_for_step_functions() == []

    @patch('lambdas.etl_invoker.lambda_function.ETLInvokerService.generate_input_for_step_functions')
    def test_lambda_success(self, generate_input):
        generate_input.return_value = "a"
        assert lambda_handler(event=None, context=None) == "a"

    @patch('lambdas.etl_invoker.lambda_function.ETLInvokerService.generate_input_for_step_functions')
    def test_lambda_failure(self, generate_input):
        generate_input.return_value = -1
        assert lambda_handler(event=None, context=None) == {
            'statusCode': 500,
            'message': json.dumps('Error in getting input for step functions')
        }
