"""
Service: roll_back_checkpoint
Module: rollback_service
Author: Sourav Hazra
"""
from helpers.dynamodb_helper import DynamoDBHelper


class RollBackService:
    """
    Rollback service to rollback checkpoint timestamp in case of failure
    """
    def __init__(self, **dependencies):
        """
        Constructor method for RollBackService
        """
        self.__dynamodb = dependencies.get("dynamodb")
        self.__logger = dependencies.get("logger")

    def rollback_checkpoint(self, **kwargs):
        """
        Rollback checkpoint timestamp to the previous value stored in DynamoDB
        :param kwargs:Dict
        :return: [None, -1]
        """
        dynamodb_helper = DynamoDBHelper(dynamodb=self.__dynamodb, logger=self.__logger)

        self.__logger.info("Getting table checkpoint")

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
            self.__logger.error("Error in getting table checkpoint")
            return -1

        if not response.get("changeFlag"):
            self.__logger.info("No modifications")
            return None

        prev_redshift_migration_checkpoint = response.get("prevRedshiftMigrationCheckpoint")
        prev_redshift_record_checkpoint = response.get("prevRedshiftRecordCheckpoint")
        prev_redshift_timestamp_checkpoint = response.get("prevRedshiftTimestampCheckpoint")

        self.__logger.info("Restoring to previous checkpoint")

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
            update_expression="set redshiftMigrationCheckpoint=:redshiftMigrationCheckpoint,"
                              "redshiftRecordCheckpoint=:redshiftRecordCheckpoint,"
                              "redshiftTimestampCheckpoint=:redshiftTimestampCheckpoint,"
                              "changeFlag=:changeFlag",
            expression_attribute_values={
                ":redshiftMigrationCheckpoint": prev_redshift_migration_checkpoint,
                ":redshiftRecordCheckpoint": prev_redshift_record_checkpoint,
                ":redshiftTimestampCheckpoint": prev_redshift_timestamp_checkpoint,
                ":changeFlag": 0
            }
        )

        if not response:
            self.__logger.error("Error in restoring to previous checkpoint")
            return -1
        return None
