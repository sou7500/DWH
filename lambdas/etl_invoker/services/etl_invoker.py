"""
Service: etl_invoker
Module: etl_invoker
Author: Sourav Hazra
"""
from datetime import datetime

from helpers.s3_helper import S3Helper


class ETLInvokerService:
    """
    Service class for ETL Invoker
    """

    def __init__(self, **dependencies):
        """
        Constructor for ETLInvokerService
        :param dependencies: Dependent AWS Services
        """
        self.__s3 = dependencies.get("s3").get("resource")
        self.__s3_bucket_name = dependencies.get("s3").get("bucket_name")
        self.__s3_database_prefix = dependencies.get("s3").get("database_prefix")
        self.__logger = dependencies.get("logger")
        self.__environment = dependencies.get("environment")

    def get_databases_to_migrate(self):
        """
        Refer to the table schema stored in S3 Bucket and get the list of databases to be migrated
        :return: [Dict, int]
        """

        self.__logger.info(
            f"Getting list of objects from S3 under {self.__s3_bucket_name}/{self.__s3_database_prefix}"
        )

        # Get the list of databases from S3
        databases = S3Helper(logger=self.__logger).list_objects(
            s3=self.__s3,
            bucket_name=self.__s3_bucket_name,
        )
        if databases == []:
            self.__logger.info(
                f"No objects found at {self.__s3_bucket_name}/{self.__s3_database_prefix}"
            )
            return []
        if not databases:
            self.__logger.error(
                f"Error encountered in getting objects from {self.__s3_bucket_name}/{self.__s3_database_prefix}"
            )
            return -1

        database_configs = list(filter(lambda x: x.startswith(self.__s3_database_prefix), databases))

        return database_configs

    def generate_input_for_step_functions(self):
        """
        Frame the input for Step Functions
        :return: [Dict, int]
        """
        try:
            monthly_backup_flag = 0
            utc_timestamp = datetime.utcnow()
            if utc_timestamp.day == 1:
                monthly_backup_flag = 1
            self.__logger.info("Getting list of databases")
            database_configs = self.get_databases_to_migrate()

            if database_configs == -1:
                self.__logger.error("Error encountered in getting list of databases")
                return -1

            step_functions_input = []
            for database_config in database_configs:
                if database_config.split(".")[-1] != "json":
                    self.__logger.info(f"{database_config} is not a json file")
                    continue
                self.__logger.info(f"Framing input for database: {database_config.split('.')[0].split('/')[-1]}")
                step_functions_input.append({
                    "input": {
                        "databaseName": database_config.split(".")[0].split("/")[-1],
                        "environment": self.__environment,
                        "configBucket": self.__s3_bucket_name,
                        "configFileKey": database_config,
                        "monthlyBackUp": monthly_backup_flag
                    }
                })
        except Exception as exception:
            self.__logger.exception(f"Exception in framing inputs for step functions: {exception}")
            return -1

        self.__logger.info(f"Step Functions input: {step_functions_input}")
        return step_functions_input
