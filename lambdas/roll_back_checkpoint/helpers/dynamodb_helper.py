"""
Service: roll_back_checkpoint
Module: dynamodb_helper
Author: Sourav Hazra
"""


class DynamoDBHelper:
    """
    DynamoDB Helper for DynamoDB operations
    """

    def __init__(self, **kwargs):
        """
        Constructor for DynamoDB Helper
        """
        self.__dynamodb = kwargs.get("dynamodb")
        self.__logger = kwargs.get("logger")

    def get_item(self, **kwargs):
        """
        Fetch a particular item from DynamoDB using partition key and sort key
        """
        key = {}
        if kwargs.get("sort_key"):
            key[kwargs.get("sort_key").get("key_name")] = kwargs.get("sort_key").get("key_value")
        key[kwargs.get("partition_key").get("key_name")] = kwargs.get("partition_key").get("key_value")
        table = self.__dynamodb.Table(kwargs.get("table_name"))
        try:
            response = table.get_item(
                Key=key
            )
            if not response:
                return None
        except Exception as exception:
            self.__logger.exception(f"Error encountered in getting item from DynamoDB: {exception}")
            return None
        return response.get("Item")

    def update_item(self, **kwargs):
        """
        Update a particular item from DynamoDB if it exists using partition key and sort key
        """
        table = self.__dynamodb.Table(kwargs.get("table_name"))
        key = {}
        if kwargs.get("sort_key"):
            key[kwargs.get("sort_key").get("key_name")] = kwargs.get("sort_key").get("key_value")
        key[kwargs.get("partition_key").get("key_name")] = kwargs.get("partition_key").get("key_value")
        try:
            response = table.update_item(
                Key=key,
                UpdateExpression=kwargs.get("update_expression"),
                ExpressionAttributeValues=kwargs.get("expression_attribute_values"),
                ConditionExpression=f"attribute_exists({kwargs.get('partition_key').get('key_name')}) AND "
                                    f"attribute_exists({kwargs.get('sort_key').get('key_name')})"
            )
            if not response:
                return None
        except Exception as exception:
            self.__logger.exception(f"Error encountered in updating item from DynamoDB: {exception}")
            return None
        return True
