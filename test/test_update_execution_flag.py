import json

import boto3
from moto import mock_dynamodb
import unittest
from unittest.mock import patch

from aws_lambda_powertools import Logger

from lambdas.update_execution_flag.helpers.dynamodb_helper import DynamoDBHelper
from lambdas.update_execution_flag.services.update_execution_flag_service import UpdateExecutionFlagService
from lambdas.update_execution_flag.lambda_function import lambda_handler

logger = Logger()


class TestUpdateExecutionFlag(unittest.TestCase):

    @mock_dynamodb
    def test_helper_get_item_exists(self):
        dynamodb_client = boto3.client('dynamodb')
        dynamodb_client.create_table(
            TableName='sample_table',
            KeySchema=[
                {
                    'AttributeName': 'a',
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName': 'b',
                    'KeyType': 'RANGE'
                },
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'a',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'b',
                    'AttributeType': 'S'
                },
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        dynamodb_client.put_item(
            TableName='sample_table',
            Item={
                'a': {
                    'S': 'x',
                },
                'b': {
                    'S': 'y',
                },
                'c': {
                    'S': 'z',
                },
            }
        )
        dynamodb = boto3.resource('dynamodb')
        dynamodb_helper = DynamoDBHelper(dynamodb=dynamodb, logger=logger)
        assert dynamodb_helper.get_item(
            table_name="sample_table",
            partition_key={
                "key_name": "a",
                "key_value": "x"
            },
            sort_key={
                "key_name": "b",
                "key_value": "y"
            }
        ) == {'a': 'x', 'b': 'y', 'c': 'z'}

    @mock_dynamodb
    def test_helper_get_item_not_exists(self):
        dynamodb_client = boto3.client('dynamodb')
        dynamodb_client.create_table(
            TableName='sample_table',
            KeySchema=[
                {
                    'AttributeName': 'a',
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName': 'b',
                    'KeyType': 'RANGE'
                },
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'a',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'b',
                    'AttributeType': 'S'
                },
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        dynamodb = boto3.resource('dynamodb')
        dynamodb_helper = DynamoDBHelper(dynamodb=dynamodb, logger=logger)
        assert dynamodb_helper.get_item(
            table_name="sample_table",
            partition_key={
                "key_name": "a",
                "key_value": "x"
            },
            sort_key={
                "key_name": "b",
                "key_value": "y"
            }
        ) is None

    @mock_dynamodb
    def test_helper_get_item_exception(self):
        dynamodb_client = boto3.client('dynamodb')
        dynamodb_client.create_table(
            TableName='sample_table',
            KeySchema=[
                {
                    'AttributeName': 'a',
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName': 'b',
                    'KeyType': 'RANGE'
                },
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'a',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'b',
                    'AttributeType': 'S'
                },
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        dynamodb = boto3.resource('dynamodb')
        dynamodb_helper = DynamoDBHelper(dynamodb=dynamodb, logger=logger)
        assert dynamodb_helper.get_item(
            table_name="sample_table1",
            partition_key={
                "key_name": "a",
                "key_value": "x"
            },
            sort_key={
                "key_name": "b",
                "key_value": "y"
            }
        ) is None

    @mock_dynamodb
    def test_helper_update_item(self):
        dynamodb_client = boto3.client('dynamodb')
        dynamodb_client.create_table(
            TableName='sample_table',
            KeySchema=[
                {
                    'AttributeName': 'a',
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName': 'b',
                    'KeyType': 'RANGE'
                },
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'a',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'b',
                    'AttributeType': 'S'
                },
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        dynamodb_client.put_item(
            TableName='sample_table',
            Item={
                'a': {
                    'S': 'x',
                },
                'b': {
                    'S': 'y',
                },
                'c': {
                    'S': 'z',
                },
            }
        )
        dynamodb = boto3.resource('dynamodb')
        dynamodb_helper = DynamoDBHelper(dynamodb=dynamodb, logger=logger)
        assert dynamodb_helper.update_item(
            table_name="sample_table",
            partition_key={
                "key_name": "a",
                "key_value": "x"
            },
            sort_key={
                "key_name": "b",
                "key_value": "y"
            },
            update_expression="set c=:c",
            expression_attribute_values={
                ":c": "b"
            }
        ) is True

    @mock_dynamodb
    def test_helper_update_item_exception(self):
        dynamodb_client = boto3.client('dynamodb')
        dynamodb_client.create_table(
            TableName='sample_table',
            KeySchema=[
                {
                    'AttributeName': 'a',
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName': 'b',
                    'KeyType': 'RANGE'
                },
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'a',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'b',
                    'AttributeType': 'S'
                },
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        dynamodb = boto3.resource('dynamodb')
        dynamodb_helper = DynamoDBHelper(dynamodb=dynamodb, logger=logger)
        assert dynamodb_helper.update_item(
            table_name="sample_table",
            partition_key={
                "key_name": "a",
                "key_value": "x"
            },
            sort_key={
                "key_name": "b",
                "key_value": "y"
            },
            update_expression="set c=:c",
            expression_attribute_values={
                ":c": "b"
            }
        ) is None

    @patch('lambdas.update_execution_flag.services.update_execution_flag_service.DynamoDBHelper.get_item')
    def test_update_execution_flag_null_get_item(self, get_item):
        get_item.return_value = None
        assert UpdateExecutionFlagService(logger=logger).update_execution_flag() == -1

    @patch('lambdas.update_execution_flag.services.update_execution_flag_service.DynamoDBHelper.get_item')
    def test_update_execution_flag_unset_change_flag(self, get_item):
        get_item.return_value = {"changeFlag": 0}
        assert UpdateExecutionFlagService(logger=logger).update_execution_flag() is None

    @patch('lambdas.update_execution_flag.services.update_execution_flag_service.DynamoDBHelper.get_item')
    def test_update_execution_flag_missing_change_flag(self, get_item):
        get_item.return_value = {"a": "b"}
        assert UpdateExecutionFlagService(logger=logger).update_execution_flag() is None

    @patch('lambdas.update_execution_flag.services.update_execution_flag_service.DynamoDBHelper.update_item')
    @patch('lambdas.update_execution_flag.services.update_execution_flag_service.DynamoDBHelper.get_item')
    def test_update_execution_flag_unsuccessful_update(self, get_item, update_item):
        get_item.return_value = {"changeFlag": 1}
        update_item.return_value = None
        assert UpdateExecutionFlagService(logger=logger).update_execution_flag() == -1

    @patch('lambdas.update_execution_flag.services.update_execution_flag_service.DynamoDBHelper.update_item')
    @patch('lambdas.update_execution_flag.services.update_execution_flag_service.DynamoDBHelper.get_item')
    def test_update_execution_flag_successful_update(self, get_item, update_item):
        get_item.return_value = {"changeFlag": 1}
        update_item.return_value = True
        assert UpdateExecutionFlagService(logger=logger).update_execution_flag() is None

    def test_lambda_no_input(self):
        assert lambda_handler(context=None, event={}) == {
            "statusCode": 500,
            "message": "Exception encountered in lambda function"
        }

    @patch('lambdas.update_execution_flag.lambda_function.UpdateExecutionFlagService.update_execution_flag')
    def test_lambda_successful_update(self, update_execution_flag):
        update_execution_flag.return_value = None
        assert lambda_handler(context=None, event={"input": {}}) == {
            'statusCode': 200,
            'message': "SUCCESS"
        }

    @patch('lambdas.update_execution_flag.lambda_function.UpdateExecutionFlagService.update_execution_flag')
    def test_lambda_unsuccessful_update(self, update_execution_flag):
        update_execution_flag.return_value = -1
        assert lambda_handler(context=None, event={"input": {}}) == {
            'statusCode': 500,
            'message': json.dumps('Error in rolling back checkpoint')
        }
