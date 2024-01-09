"""
Service: update_execution_flag
Module: update_execution_flag_service
Author: Sourav Hazra
"""
from helpers.dynamodb_helper import DynamoDBHelper


class UpdateExecutionFlagService:
    """
    UpdateExecutionFlagService to update execution flag after successful migration from S3 to Redshift
    """
    def __init__(self, **dependencies):
        """
        Constructor method for UpdateExecutionFlagService
        """
        self.__dynamodb = dependencies.get("dynamodb")
        self.__logger = dependencies.get("logger")

    def update_execution_flag(self, **kwargs):
        """
        Update execution flag
        :param kwargs: Dict
        :return [None, -1]
        """
        dynamodb_helper = DynamoDBHelper(dynamodb=self.__dynamodb, logger=self.__logger)

        self.__logger.info("Getting checkpoint")

        response = dynamodb_helper.get_item(
            table_name=kwargs.get("checkpoint_table_name"),
            partition_key={
                "key_name": "databaseName",
                "key_value": kwargs.get("database_name")
            },
            sort_key={
                "key_name": "tableName",
                "key_value": kwargs.get("table_name")
            }
        )

        if not response:
            self.__logger.error("Error in getting checkpoint")
            return -1

        if not response.get("changeFlag"):
            self.__logger.info("No modifications")
            return None

        self.__logger.info("Updating execution flag")

        response = dynamodb_helper.update_item(
            table_name=kwargs.get("checkpoint_table_name"),
            partition_key={
                "key_name": "databaseName",
                "key_value": kwargs.get("database_name")
            },
            sort_key={
                "key_name": "tableName",
                "key_value": kwargs.get("table_name")
            },
            update_expression="set changeFlag=:changeFlag",
            expression_attribute_values={
                ":changeFlag": 0
            }
        )

        if not response:
            self.__logger.error("Error in updating execution flag")
            return -1
        return None
