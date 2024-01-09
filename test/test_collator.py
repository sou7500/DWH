import unittest
from lambdas.collator.lambda_function import lambda_handler


class TestCollator(unittest.TestCase):

    def test_lambda_handler_with_no_jobRunnerOutput(self):
        test_event = {
        }
        test_context = None
        expected_output = {
            "error": {
                "statusCode": 500,
                "message": "Exception encountered in lambda function"
            }
        }

        assert lambda_handler(event=test_event, context=test_context) == expected_output

    def test_lambda_handler_with_success_statusCode_with_no_input(self):
        test_event = {
            "jobRunnerOutput": [
                {
                    "statusCode": 200
                },
                {
                    "statusCode": 200
                }
            ]
        }
        test_context = None
        expected_output = {}

        assert lambda_handler(event=test_event, context=test_context) == expected_output

    def test_lambda_handler_with_error_statusCode_with_no_input(self):
        test_event = {
            "jobRunnerOutput": [
                {
                    "statusCode": 200
                },
                {
                    "statusCode": 500
                }
            ]
        }
        test_context = None
        expected_output = {
                "error": {
                    "statusCode": 500,
                    "message": "Parallel job failed"
                }
            }
        assert lambda_handler(event=test_event, context=test_context) == expected_output

    def test_lambda_handler_with_success_statusCode_with_input(self):
        test_event = {
            "input": {
                "abc": "xyz"
            },
            "jobRunnerOutput": [
                {
                    "statusCode": 200
                },
                {
                    "statusCode": 200
                }
            ]
        }
        test_context = None
        expected_output = {
            "input": {
                "abc": "xyz"
            }
        }

        assert lambda_handler(event=test_event, context=test_context) == expected_output

    def test_lambda_handler_with_no_job_runs_with_input(self):
        test_event = {
            "input": {
                "abc": "xyz"
            },
            "jobRunnerOutput": [

            ]
        }
        test_context = None
        expected_output = {
            "input": {
                "abc": "xyz"
            }
        }

        assert lambda_handler(event=test_event, context=test_context) == expected_output
