import json
import unittest
from unittest.mock import patch
import boto3
from aws_lambda_powertools import Logger
from moto import mock_s3

from lambdas.check_columns.helpers.s3_helper import S3Helper
from lambdas.check_columns.services.redshift_service import RedshiftService
from lambdas.check_columns.lambda_function import lambda_handler

logger = Logger()


class TestCheckColumns(unittest.TestCase):
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

    @patch('lambdas.check_columns.services.redshift_service.S3Helper.fetch_object')
    def test_get_table_schema_from_definition_unsuccessful_fetch(self, fetch_object):
        fetch_object.return_value = None
        redshift_service = RedshiftService(
            redshift=None,
            s3={},
            redshift_params={},
            logger=logger
        )
        assert redshift_service.get_table_schema_from_definition() == -1

    @patch('lambdas.check_columns.services.redshift_service.S3Helper.fetch_object')
    def test_get_table_schema_from_definition_unsuccessful_parse(self, fetch_object):
        fetch_object.return_value = "abc"
        redshift_service = RedshiftService(
            redshift=None,
            s3={},
            redshift_params={},
            logger=logger
        )
        assert redshift_service.get_table_schema_from_definition() is None

    @patch('lambdas.check_columns.services.redshift_service.S3Helper.fetch_object')
    def test_get_table_schema_from_definition_successful_fetch(self, fetch_object):
        fetch_object.return_value = json.dumps({"key": "value"})
        redshift_service = RedshiftService(
            redshift=None,
            s3={},
            redshift_params={},
            logger=logger
        )
        assert redshift_service.get_table_schema_from_definition() == {"key": "value"}

    def test_add_column_unsuccessful_add_column_invalid_params(self):
        redshift_service = RedshiftService(
            redshift=None,
            s3={},
            redshift_params={},
            logger=logger
        )
        assert redshift_service._RedshiftService__add_column(
            None, None, None, None
        ) is None

    @patch('lambdas.check_columns.services.redshift_service.RedshiftHelper.run_query')
    def test_add_column_unsuccessful_add_column(self, run_query):
        run_query.return_value = None
        redshift_service = RedshiftService(
            redshift=None,
            s3={},
            redshift_params={},
            logger=logger
        )
        assert redshift_service._RedshiftService__add_column(
            None, None, None, {"a": "b", "c": "d"}
        ) is None

    @patch('lambdas.check_columns.services.redshift_service.RedshiftHelper.run_query')
    def test_add_column_successful_add_column(self, run_query):
        run_query.return_value = "some-id"
        redshift_service = RedshiftService(
            redshift=None,
            s3={},
            redshift_params={},
            logger=logger
        )
        assert redshift_service._RedshiftService__add_column(
            None, None, None, {"a": "b", "c": "d"}
        ) is True

    @patch('lambdas.check_columns.services.redshift_service.RedshiftHelper.run_query')
    def test_add_column_successful_no_column_to_add(self, run_query):
        run_query.return_value = "some-id"
        redshift_service = RedshiftService(
            redshift=None,
            s3={},
            redshift_params={},
            logger=logger
        )
        assert redshift_service._RedshiftService__add_column(
            None, None, None, {}
        ) is True

    def test_drop_column_unsuccessful_add_column_invalid_params(self):
        redshift_service = RedshiftService(
            redshift=None,
            s3={},
            redshift_params={},
            logger=logger
        )
        assert redshift_service._RedshiftService__drop_column(
            None, None, None, None
        ) is None

    @patch('lambdas.check_columns.services.redshift_service.RedshiftHelper.run_query')
    def test_drop_column_unsuccessful_drop_column(self, run_query):
        run_query.return_value = None
        redshift_service = RedshiftService(
            redshift=None,
            s3={},
            redshift_params={},
            logger=logger
        )
        assert redshift_service._RedshiftService__drop_column(
            None, None, None, ["a"]
        ) is None

    @patch('lambdas.check_columns.services.redshift_service.RedshiftHelper.run_query')
    def test_drop_column_successful_no_column_to_drop(self, run_query):
        run_query.return_value = "some-id"
        redshift_service = RedshiftService(
            redshift=None,
            s3={},
            redshift_params={},
            logger=logger
        )
        assert redshift_service._RedshiftService__drop_column(
            None, None, None, []
        ) is True

    @patch('lambdas.check_columns.services.redshift_service.RedshiftHelper.run_query')
    def test_drop_column_successful_drop_column(self, run_query):
        run_query.return_value = "some-id"
        redshift_service = RedshiftService(
            redshift=None,
            s3={},
            redshift_params={},
            logger=logger
        )
        assert redshift_service._RedshiftService__drop_column(
            None, None, None, ["a", "b"]
        ) is True

    @patch('lambdas.check_columns.services.redshift_service.RedshiftHelper.run_query')
    def test_make_table_consistent_with_definition_query_error(self, run_query):
        run_query.return_value = None
        redshift_service = RedshiftService(
            redshift=None,
            s3={},
            redshift_params={},
            logger=logger
        )
        assert redshift_service.make_table_consistent_with_definition(
            None, None, None, None
        ) == -1

    @patch.object(RedshiftService, '_RedshiftService__add_column')
    @patch.object(RedshiftService, '_RedshiftService__drop_column')
    @patch('lambdas.check_columns.services.redshift_service.RedshiftHelper.get_query_results')
    @patch('lambdas.check_columns.services.redshift_service.RedshiftHelper.run_query')
    def test_make_table_consistent_with_definition_success(self, run_query, get_query_results, drop_column, add_column):
        run_query.return_value = "some-id"
        get_query_results.return_value = [
            [
                {}, {}, {"a": "b", "migration_type": "x", "d": "e"}
            ]
        ]
        add_column.return_value = True
        drop_column.return_value = True
        redshift_service = RedshiftService(
            redshift=None,
            s3={},
            redshift_params={},
            logger=logger
        )
        assert redshift_service.make_table_consistent_with_definition(
            None, None, None, {"columns": {"a": "b", "c": "d"}}
        ) is True

    @patch.object(RedshiftService, '_RedshiftService__add_column')
    @patch.object(RedshiftService, '_RedshiftService__drop_column')
    @patch('lambdas.check_columns.services.redshift_service.RedshiftHelper.get_query_results')
    @patch('lambdas.check_columns.services.redshift_service.RedshiftHelper.run_query')
    def test_make_table_consistent_with_definition_drop_column_failure(self, run_query, get_query_results, drop_column,
                                                                       add_column):
        run_query.return_value = "some-id"
        get_query_results.return_value = [
            [
                {}, {}, {"a": "b", "migration_type": "x", "d": "e"}
            ]
        ]
        add_column.return_value = True
        drop_column.return_value = False
        redshift_service = RedshiftService(
            redshift=None,
            s3={},
            redshift_params={},
            logger=logger
        )
        assert redshift_service.make_table_consistent_with_definition(
            None, None, None, {"columns": {"a": "b", "c": "d"}}
        ) == -2

    @patch.object(RedshiftService, '_RedshiftService__add_column')
    @patch.object(RedshiftService, '_RedshiftService__drop_column')
    @patch('lambdas.check_columns.services.redshift_service.RedshiftHelper.get_query_results')
    @patch('lambdas.check_columns.services.redshift_service.RedshiftHelper.run_query')
    def test_make_table_consistent_with_definition_add_column_failure(self, run_query, get_query_results, drop_column,
                                                                      add_column):
        run_query.return_value = "some-id"
        get_query_results.return_value = [
            [
                {}, {}, {"a": "b", "migration_type": "x", "d": "e"}
            ]
        ]
        add_column.return_value = False
        drop_column.return_value = True
        redshift_service = RedshiftService(
            redshift=None,
            s3={},
            redshift_params={},
            logger=logger
        )
        assert redshift_service.make_table_consistent_with_definition(
            None, None, None, {"columns": {"a": "b", "c": "d"}}
        ) == -2

    @patch.object(RedshiftService, '_RedshiftService__add_column')
    @patch.object(RedshiftService, '_RedshiftService__drop_column')
    @patch('lambdas.check_columns.services.redshift_service.RedshiftHelper.get_query_results')
    @patch('lambdas.check_columns.services.redshift_service.RedshiftHelper.run_query')
    def test_make_table_consistent_with_definition_all_failure(self, run_query, get_query_results, drop_column,
                                                               add_column):
        run_query.return_value = "some-id"
        get_query_results.return_value = [
            [
                {}, {}, {"a": "b", "migration_type": "x", "d": "e"}
            ]
        ]
        add_column.return_value = False
        drop_column.return_value = False
        redshift_service = RedshiftService(
            redshift=None,
            s3={},
            redshift_params={},
            logger=logger
        )
        assert redshift_service.make_table_consistent_with_definition(
            None, None, None, {"columns": {"a": "b", "c": "d"}}
        ) == -2

    @patch('lambdas.check_columns.lambda_function.RedshiftService.get_table_schema_from_definition')
    def test_lambda_handler_schema_fetch_failure(self, get_table_schema_from_definition):
        get_table_schema_from_definition.return_value = -1
        expected_output = {
            'statusCode': 500,
            'message': json.dumps('Error in fetching schema for table')
        }
        assert lambda_handler(event={"input": {}}, context=None) == expected_output

    @patch('lambdas.check_columns.lambda_function.RedshiftService.make_table_consistent_with_definition')
    @patch('lambdas.check_columns.lambda_function.RedshiftService.get_table_schema_from_definition')
    def test_lambda_handler_main_table_schema_fetch_failure_from_redshift(self, get_table_schema_from_definition,
                                                                          make_table_consistent_with_definition):
        get_table_schema_from_definition.return_value = ""
        make_table_consistent_with_definition.return_value = -1
        expected_output = {
            'statusCode': 500,
            'message': json.dumps('Error in getting schema from Redshift')
        }
        assert lambda_handler(event={"input": {}}, context=None) == expected_output

    @patch('lambdas.check_columns.lambda_function.RedshiftService.make_table_consistent_with_definition')
    @patch('lambdas.check_columns.lambda_function.RedshiftService.get_table_schema_from_definition')
    def test_lambda_handler_main_table_error_in_add_delete_columns(self, get_table_schema_from_definition,
                                                                   make_table_consistent_with_definition):
        get_table_schema_from_definition.return_value = ""
        make_table_consistent_with_definition.return_value = -2
        expected_output = {
            'statusCode': 500,
            'message': json.dumps('Error in adding/deleting columns')
        }
        assert lambda_handler(event={"input": {}}, context=None) == expected_output

    @patch('lambdas.check_columns.lambda_function.RedshiftService.make_table_consistent_with_definition')
    @patch('lambdas.check_columns.lambda_function.RedshiftService.get_table_schema_from_definition')
    def test_lambda_handler_staging_table_schema_fetch_failure_from_redshift(self, get_table_schema_from_definition,
                                                                             make_table_consistent_with_definition):
        get_table_schema_from_definition.return_value = ""
        make_table_consistent_with_definition.side_effect = [True, -1]
        expected_output = {
            'statusCode': 500,
            'message': json.dumps('Error in getting schema from Redshift')
        }
        assert lambda_handler(event={"input": {}}, context=None) == expected_output

    @patch('lambdas.check_columns.lambda_function.RedshiftService.make_table_consistent_with_definition')
    @patch('lambdas.check_columns.lambda_function.RedshiftService.get_table_schema_from_definition')
    def test_lambda_handler_staging_table_error_in_add_delete_columns(self, get_table_schema_from_definition,
                                                                      make_table_consistent_with_definition):
        get_table_schema_from_definition.return_value = ""
        make_table_consistent_with_definition.side_effect = [True, -2]
        expected_output = {
            'statusCode': 500,
            'message': json.dumps('Error in adding/deleting columns')
        }
        assert lambda_handler(event={"input": {}}, context=None) == expected_output

    def test_lambda_handler_no_input(self):
        expected_output = {
            "statusCode": 500,
            "message": "Exception encountered in lambda function"
        }
        assert lambda_handler(event={}, context=None) == expected_output

    @patch('lambdas.check_columns.lambda_function.RedshiftService.make_table_consistent_with_definition')
    @patch('lambdas.check_columns.lambda_function.RedshiftService.get_table_schema_from_definition')
    def test_lambda_handler_success(self, get_table_schema_from_definition, make_table_consistent_with_definition):
        get_table_schema_from_definition.return_value = ""
        make_table_consistent_with_definition.side_effect = [True, True]
        expected_output = {
            'statusCode': 200,
            'message': "SUCCESS"
        }
        assert lambda_handler(event={"input": {}}, context=None) == expected_output
