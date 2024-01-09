import json
import unittest
from unittest.mock import patch
import boto3
from aws_lambda_powertools import Logger
from moto import mock_s3

from lambdas.execute_sql.helpers.s3_helper import S3Helper
from lambdas.execute_sql.services.redshift_service import RedshiftService
from lambdas.execute_sql.lambda_function import lambda_handler

logger = Logger()


class TestExecuteSQL(unittest.TestCase):
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

    @patch('lambdas.execute_sql.services.redshift_service.S3Helper.fetch_object')
    def test_fetch_sql_statement_unsuccessful_fetch(self, fetch_object):
        fetch_object.return_value = None
        redshift_service = RedshiftService(redshift=None, s3={}, redshift_params={}, logger=logger)
        assert redshift_service._RedshiftService__fetch_sql_statement() is None

    @patch('lambdas.execute_sql.services.redshift_service.S3Helper.fetch_object')
    def test_fetch_sql_statement_empty_string(self, fetch_object):
        fetch_object.return_value = ""
        redshift_service = RedshiftService(redshift=None, s3={}, redshift_params={}, logger=logger)
        assert redshift_service._RedshiftService__fetch_sql_statement() is None

    @patch('lambdas.execute_sql.services.redshift_service.S3Helper.fetch_object')
    def test_fetch_sql_statement_successful_fetch(self, fetch_object):
        fetch_object.return_value = "sql"
        redshift_service = RedshiftService(redshift=None, s3={}, redshift_params={}, logger=logger)
        assert redshift_service._RedshiftService__fetch_sql_statement() == "sql"

    @patch('lambdas.execute_sql.services.redshift_service.RedshiftHelper.run_query')
    def test_run_sql_query_unsuccessful(self, run_query):
        run_query.return_value = None
        redshift_service = RedshiftService(redshift=None, s3={}, redshift_params={}, logger=logger)
        assert redshift_service._RedshiftService__run_sql_query(sql_query="") is None

    @patch('lambdas.execute_sql.services.redshift_service.RedshiftHelper.run_query')
    def test_run_sql_query_successful(self, run_query):
        run_query.return_value = "some-id"
        redshift_service = RedshiftService(redshift=None, s3={}, redshift_params={}, logger=logger)
        assert redshift_service._RedshiftService__run_sql_query(sql_query="") == "some-id"

    @patch.object(RedshiftService, '_RedshiftService__fetch_sql_statement')
    def test_create_stored_procedure_unsuccessful_fetch(self, fetch_sql_statement):
        fetch_sql_statement.return_value = None
        redshift_service = RedshiftService(redshift=None, s3={}, redshift_params={}, logger=logger)
        assert redshift_service._RedshiftService__create_stored_procedure() is None

    @patch.object(RedshiftService, '_RedshiftService__run_sql_query')
    @patch.object(RedshiftService, '_RedshiftService__fetch_sql_statement')
    def test_create_stored_procedure_unsuccessful_query_run(self, fetch_sql_statement, run_sql_query):
        fetch_sql_statement.return_value = "some value"
        run_sql_query.return_value = None
        redshift_service = RedshiftService(redshift=None, s3={}, redshift_params={}, logger=logger)
        assert redshift_service._RedshiftService__create_stored_procedure() is None

    @patch.object(RedshiftService, '_RedshiftService__run_sql_query')
    @patch.object(RedshiftService, '_RedshiftService__fetch_sql_statement')
    def test_create_stored_procedure_invalid_porc_name(self, fetch_sql_statement, run_sql_query):
        fetch_sql_statement.return_value = "CREATE OR REPLACE PROCEDURE procedure"
        run_sql_query.return_value = "some-id"
        redshift_service = RedshiftService(redshift=None, s3={}, redshift_params={}, logger=logger)
        assert redshift_service._RedshiftService__create_stored_procedure() is None

    @patch.object(RedshiftService, '_RedshiftService__run_sql_query')
    @patch.object(RedshiftService, '_RedshiftService__fetch_sql_statement')
    def test_create_stored_procedure_success(self, fetch_sql_statement, run_sql_query):
        fetch_sql_statement.return_value = "CREATE OR REPLACE PROCEDURE procedure("
        run_sql_query.return_value = "some-id"
        redshift_service = RedshiftService(redshift=None, s3={}, redshift_params={}, logger=logger)
        assert redshift_service._RedshiftService__create_stored_procedure() == "procedure"

    @patch.object(RedshiftService, '_RedshiftService__create_stored_procedure')
    def test_invoke_stored_procedure_create_proc_unsuccessful(self, create_stored_procedure):
        create_stored_procedure.return_value = None
        redshift_service = RedshiftService(redshift=None, s3={}, redshift_params={}, logger=logger)
        assert redshift_service._RedshiftService__invoke_stored_procedure() == -1

    @patch.object(RedshiftService, '_RedshiftService__run_sql_query')
    @patch.object(RedshiftService, '_RedshiftService__create_stored_procedure')
    def test_invoke_stored_procedure_execute_proc_unsuccessful(self, create_stored_procedure, run_sql_query):
        create_stored_procedure.return_value = "procedure"
        run_sql_query.return_value = None
        redshift_service = RedshiftService(redshift=None, s3={}, redshift_params={}, logger=logger)
        assert redshift_service._RedshiftService__invoke_stored_procedure() == -2

    @patch.object(RedshiftService, '_RedshiftService__run_sql_query')
    @patch.object(RedshiftService, '_RedshiftService__create_stored_procedure')
    def test_invoke_stored_procedure_execute_proc_unsuccessful(self, create_stored_procedure, run_sql_query):
        create_stored_procedure.return_value = "procedure"
        run_sql_query.return_value = None
        redshift_service = RedshiftService(redshift=None, s3={}, redshift_params={}, logger=logger)
        assert redshift_service._RedshiftService__invoke_stored_procedure() == -2

    @patch.object(RedshiftService, '_RedshiftService__run_sql_query')
    @patch.object(RedshiftService, '_RedshiftService__create_stored_procedure')
    def test_invoke_stored_procedure_successful(self, create_stored_procedure, run_sql_query):
        create_stored_procedure.return_value = "procedure"
        run_sql_query.return_value = "some-id"
        redshift_service = RedshiftService(redshift=None, s3={}, redshift_params={}, logger=logger)
        assert redshift_service._RedshiftService__invoke_stored_procedure() == "some-id"

    @patch.object(RedshiftService, '_RedshiftService__invoke_stored_procedure')
    def test_run_sql_statement_successful_proc(self, invoke_stored_procedure):
        invoke_stored_procedure.return_value = "some-id"
        redshift_service = RedshiftService(redshift=None, s3={}, redshift_params={}, logger=logger)
        assert redshift_service.run_sql_statement(type="PROC", proc_arguments="") == "some-id"

    @patch.object(RedshiftService, '_RedshiftService__run_sql_query')
    def test_run_sql_statement_unsuccessful_ad_hoc(self, run_sql_query):
        run_sql_query.return_value = None
        redshift_service = RedshiftService(redshift=None, s3={}, redshift_params={}, logger=logger)
        assert redshift_service.run_sql_statement(type="ADHOC", proc_arguments="") == -2

    @patch.object(RedshiftService, '_RedshiftService__run_sql_query')
    def test_run_sql_statement_successful_ad_hoc(self, run_sql_query):
        run_sql_query.return_value = "some-id"
        redshift_service = RedshiftService(redshift=None, s3={}, redshift_params={}, logger=logger)
        assert redshift_service.run_sql_statement(type="ADHOC", proc_arguments="") == "some-id"

    @patch.object(RedshiftService, '_RedshiftService__fetch_sql_statement')
    def test_run_sql_statement_fetch_and_run_unsuccessful_fetch(self, fetch_sql_statement):
        fetch_sql_statement.return_value = None
        redshift_service = RedshiftService(redshift=None, s3={}, redshift_params={}, logger=logger)
        assert redshift_service.run_sql_statement() == -1

    @patch.object(RedshiftService, '_RedshiftService__fetch_sql_statement')
    def test_run_sql_statement_fetch_and_run_unsuccessful_fetch(self, fetch_sql_statement):
        fetch_sql_statement.return_value = None
        redshift_service = RedshiftService(redshift=None, s3={}, redshift_params={}, logger=logger)
        assert redshift_service.run_sql_statement() == -1

    @patch.object(RedshiftService, '_RedshiftService__run_sql_query')
    @patch.object(RedshiftService, '_RedshiftService__fetch_sql_statement')
    def test_run_sql_statement_fetch_and_run_unsuccessful_run(self, fetch_sql_statement, run_sql_query):
        fetch_sql_statement.return_value = "some_query"
        run_sql_query.return_value = None
        redshift_service = RedshiftService(redshift=None, s3={}, redshift_params={}, logger=logger)
        assert redshift_service.run_sql_statement() == -2

    @patch.object(RedshiftService, '_RedshiftService__run_sql_query')
    @patch.object(RedshiftService, '_RedshiftService__fetch_sql_statement')
    def test_run_sql_statement_fetch_and_run_unsuccessful_run(self, fetch_sql_statement, run_sql_query):
        fetch_sql_statement.return_value = "some_query"
        run_sql_query.return_value = "some-id"
        redshift_service = RedshiftService(redshift=None, s3={}, redshift_params={}, logger=logger)
        assert redshift_service.run_sql_statement() == "some-id"

    def test_lambda_handler_no_input(self):
        expected_output = {
            "statusCode": 500,
            "message": "Exception encountered in lambda function"
        }
        assert lambda_handler(context=None, event={}) == expected_output

    def test_lambda_handler_no_sql_statement_key(self):
        expected_output = {
            'statusCode': 404,
            'message': json.dumps('SQL query key name not found')
        }
        assert lambda_handler(context=None, event={"input": {}}) == expected_output

    def test_lambda_handler_no_proc_key(self):
        expected_output = {
            'statusCode': 404,
            'message': json.dumps('Stored Procedure key name not found')
        }
        assert lambda_handler(context=None, event={"input": {
            "sqlStatementType": "PROC"
        }}) == expected_output

    def test_lambda_handler_no_query(self):
        expected_output = {
            'statusCode': 404,
            'message': json.dumps('SQL query not found')
        }
        assert lambda_handler(context=None, event={"input": {
            "sqlStatementType": "ADHOC"
        }}) == expected_output

    def test_lambda_handler_sql_no_sql_file(self):
        expected_output = {
            'statusCode': 200,
            'message': json.dumps('Not a S3 object')
        }
        assert lambda_handler(context=None, event={"input": {
            "sqlStatementKey": "some/key"
        }}) == expected_output

    @patch('lambdas.execute_sql.lambda_function.RedshiftService.run_sql_statement')
    def test_lambda_handler_unsuccessful_sql_query_read(self, run_sql_statement):
        run_sql_statement.return_value = -1
        expected_output = {
            'statusCode': 404,
            'message': json.dumps('Error in reading SQL query')
        }
        assert lambda_handler(context=None, event={"input": {
            "sqlStatementKey": "some/key.sql"
        }}) == expected_output

    @patch('lambdas.execute_sql.lambda_function.RedshiftService.run_sql_statement')
    def test_lambda_handler_unsuccessful_sql_query_execution(self, run_sql_statement):
        run_sql_statement.return_value = -2
        expected_output = {
            'statusCode': 500,
            'message': json.dumps('Error in executing SQL query')
        }
        assert lambda_handler(context=None, event={"input": {
            "sqlStatementKey": "some/key.sql"
        }}) == expected_output

    @patch('lambdas.execute_sql.lambda_function.RedshiftService.run_sql_statement')
    def test_lambda_handler_success(self, run_sql_statement):
        run_sql_statement.return_value = "some-query-id"
        expected_output = {
            'statusCode': 200,
            'message': "SUCCESS"
        }
        assert lambda_handler(context=None, event={"input": {
            "sqlStatementKey": "some/key.sql"
        }}) == expected_output
