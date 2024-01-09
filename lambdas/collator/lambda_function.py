"""
Service: collator
Module: lambda_function
Author: Sourav Hazra
"""
from aws_lambda_powertools import Logger

logger = Logger(service="Collator")


def lambda_handler(event, context):
    """
    Get the status code of all the parallel executions and check if all parallel executions
    have executed successfully. If all have status code 200, extract the input from the event
    and pass it to output else if a single execution gives status code as 500, return error
    message
    :param event: Dict
    :param context: Dict
    :return: Dict
    """
    try:
        job_outputs = event.get("jobRunnerOutput")
        database_name = event.get('input').get('databaseName') if event.get('input') else None

        logger.append_keys(database_name=database_name)

        logger.info("Collating outputs from parallel run")

        for job_output in job_outputs:
            logger.info("Parallel run executed successfully")
            if job_output.get("statusCode") != 200:
                logger.error("Error in executing parallel run")
                return {
                    "error": {
                        "statusCode": 500,
                        "message": "Parallel job failed"
                    }
                }
        event.pop("jobRunnerOutput")
        return event
    except Exception as exception:
        logger.exception(f"Exception encountered in lambda function: {exception}")
        return {
            "error": {
                "statusCode": 500,
                "message": "Exception encountered in lambda function"
            }
        }
    
    