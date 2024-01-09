"""
Service: incremental_load
Module: redshift_service
Author: Sourav Hazra
"""
import json

from helpers.redshift_helper import RedshiftHelper
from helpers.s3_helper import S3Helper


class RedshiftService:
    """
    Redshift Service for implementing business logic related to Redshift
    """

    def __init__(self, redshift, **dependencies):
        self.__s3 = dependencies.get("s3").get("resource")
        self.__s3_bucket_name = dependencies.get("s3").get("bucket_name")
        self.__s3_key_name = dependencies.get("s3").get("s3_key_name")
        self.__s3_schema_key = dependencies.get("s3").get("s3_schema_key")
        self.__redshift = redshift
        self.__logger = dependencies.get("logger")
        self.__database_name = dependencies.get("redshift_params").get("database_name")
        self.cluster_identifier = dependencies.get("redshift_params").get("cluster_identifier")
        self.cluster_credentials_secret = dependencies.get("redshift_params").get("cluster_credentials_secret")

    def create_incremental_load_procedure(self):
        """
         Fetch stored procedure stored in S3 for incremental load and create the stored procedure
         in Redshift
        :return: [str, int]
        """

        self.__logger.info(
            f"Fetching IncrementalLoad stored procedure using key {self.__s3_key_name} from {self.__s3_bucket_name}"
        )

        sql_query = S3Helper(logger=self.__logger).fetch_object(
            s3=self.__s3,
            bucket_name=self.__s3_bucket_name,
            key=self.__s3_key_name
        )

        if not sql_query:
            self.__logger.error(
                f"Error in fetching CreateTable stored procedure using key {self.__s3_key_name} from {self.__s3_bucket_name}"
            )
            return -1

        try:
            proc_name = sql_query.split(" ")[4][:sql_query.split(" ")[4].index('(')].strip()
        except Exception as exception:
            self.__logger.exception(f"Error in getting procedure name: {exception}")
            return -3

        self.__logger.info(f"{proc_name} definition fetched successfully")

        query_results = RedshiftHelper(redshift=self.__redshift, logger=self.__logger).run_query(
            database=self.__database_name,
            cluster_credentials_secret=self.cluster_credentials_secret,
            query=sql_query,
            cluster_identifier=self.cluster_identifier

        )

        if not query_results:
            self.__logger.error(
                f"Error in creating stored procedure {proc_name}"
            )
            return -2

        self.__logger.info(f"{proc_name} definition created successfully")

        return proc_name

    def execute_incremental_load_stored_procedure(self, **proc_args):
        """
        Execute the incremental load stored procedure
        :param proc_args: Dict
        :return: [True, -1 ,-2 ,-3, -4]
        """
        self.__logger.info(
            f"Fetching schema from {self.__s3_schema_key}"
        )
        schema = S3Helper(logger=self.__logger).fetch_object(
            s3=self.__s3,
            bucket_name=self.__s3_bucket_name,
            key=self.__s3_schema_key
        )

        if not schema:
            self.__logger.error(f"Error in fetching schema from {self.__s3_schema_key}")
            return -1

        schema = json.loads(schema)

        self.__logger.info(
            f"Getting primary key from {self.__s3_schema_key} config file"
        )
        primary_key = None
        if schema.get("tableConfigurations"):
            primary_key = schema.get("tableConfigurations").get("primaryKey")

            if not primary_key:
                self.__logger.error(
                    f"No primary key found in {self.__s3_schema_key} config file"
                )
                return -2
            self.__logger.info(
                f"Primary key {primary_key} found in {self.__s3_schema_key} config file"
            )
        else:
            self.__logger.info("No table configurations found in config file")

        proc_name = proc_args.get("proc_name")
        schema_name = proc_args.get("schema")
        table_name = proc_args.get("table")
        staging_table_name = proc_args.get("staging_table")

        sql_query = f"CALL {proc_name}('{staging_table_name}', '{table_name}', '{self.__database_name}', '{schema_name}', '{primary_key}');"

        self.__logger.info(f"Invoking stored procedure using: {sql_query}")

        query_results = RedshiftHelper(redshift=self.__redshift, logger=self.__logger).run_query(
            database=self.__database_name,
            cluster_credentials_secret=self.cluster_credentials_secret,
            query=sql_query,
            cluster_identifier=self.cluster_identifier

        )

        if not query_results:
            self.__logger.info(f"Error in executing stored procedure {proc_name}")
            return -3

        return query_results
