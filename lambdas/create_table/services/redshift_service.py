"""
Service: create_table
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
        self.__s3_create_proc_key = dependencies.get("s3").get("s3_create_proc_key")
        self.__s3_schema_key = dependencies.get("s3").get("s3_schema_key")
        self.__redshift = redshift
        self.__logger = dependencies.get("logger")
        self.__database_name = dependencies.get("redshift_params").get("database_name")
        self.cluster_identifier = dependencies.get("redshift_params").get("cluster_identifier")
        self.cluster_credentials_secret = dependencies.get("redshift_params").get("cluster_credentials_secret")

    def frame_create_table_stored_procedure(self):
        """
        Refer to the table schema stored in S3 Bucket and frame the create table stored procedure
        :return: [str, int]
        """

        self.__logger.info(f"Getting schema from S3 for Redshift table using key: {self.__s3_schema_key}")

        # Get the table schema from S3
        schema = S3Helper(logger=self.__logger).fetch_object(
            s3=self.__s3,
            bucket_name=self.__s3_bucket_name,
            key=self.__s3_schema_key
        )

        if not schema:
            self.__logger.error(f"Error in getting data from S3 using key: {self.__s3_schema_key}")
            return -1

        schema = json.loads(schema)

        # Frame the table columns along with their corresponding data type
        table_columns = ""
        for column_name, data_type in schema.get("columns").items():
            data_type = "varchar(65535)" if data_type == "varchar" else data_type
            table_columns += f'"{column_name}" {data_type},'

        table_columns = table_columns[:-1]
        # Frame the table constraints like primary key, foreign key etc.
        table_constraints = ""
        if schema.get("tableConfigurations"):
            for attr, value in schema.get("tableConfigurations").items():
                temp = ""
                for k in attr:
                    if k.isupper():
                        temp += f" {k.lower()}"
                    else:
                        temp += k
                table_constraints += f'{temp}("{value}"),'

        table_schema = f"{table_columns},{table_constraints}"[:-1]

        # Frame the table redshift configurations like compound sort key, distribution key etc.
        redshift_config = ""
        if schema.get("redshiftConfigurations"):
            for attr, value in schema.get("redshiftConfigurations").items():
                temp = ""
                for k in attr:
                    if k.isupper():
                        temp += f" {k.lower()}"
                    else:
                        temp += k
                redshift_config += f'{temp}("{value}") '
            redshift_config = redshift_config.strip()

        self.__logger.info(f"Getting stored procedure from S3 using key: {self.__s3_create_proc_key}")

        # Fetch the dynamically editable stored procedure from S3
        sql_query = S3Helper(logger=self.__logger).fetch_object(
            s3=self.__s3,
            bucket_name=self.__s3_bucket_name,
            key=self.__s3_create_proc_key
        )

        if not sql_query:
            self.__logger.error(f"Error in getting stored procedure from S3 using key: {self.__s3_schema_key}")
            return -1

        # Extract the stored procedure name from the procedure
        try:
            proc_name = sql_query.split(" ")[4][:sql_query.split(" ")[4].index('(')].strip()
        except Exception as exception:
            self.__logger.exception(f"Error in getting procedure name: {exception}")
            return -3

        self.__logger.info(f"Creating stored procedure {proc_name}")

        # Execute the stored procedure to create the table
        query_results = RedshiftHelper(redshift=self.__redshift, logger=self.__logger).run_query(
            database=self.__database_name,
            cluster_credentials_secret=self.cluster_credentials_secret,
            query=sql_query.format(table_schema=table_schema, redshift_config=redshift_config),
            cluster_identifier=self.cluster_identifier
        )

        if not query_results:
            self.__logger.error(f"Error in creating stored procedure {proc_name}")
            return -2

        return proc_name

    def execute_create_table_stored_procedure(self, **proc_args):
        """
        Execute the stored procedure to create tables in Redshift
        :param proc_args: Dict
        :return: [JSON, int]
        """
        proc_name = proc_args.get("proc_name")
        schema_name = proc_args.get("schema")
        table_name = proc_args.get("table")
        staging_table_name = proc_args.get("staging_table")

        # Frame SQL query to invoke the stored procedure
        sql_query = f"CALL {proc_name}('{staging_table_name}', " \
                    f"'{table_name}', '{self.__database_name}'," \
                    f" '{schema_name}');"

        self.__logger.info(f"Calling stored procedure: {sql_query}")

        # Run the SQL query to invoke the stored procedure
        query_results = RedshiftHelper(redshift=self.__redshift, logger=self.__logger).run_query(
            database=self.__database_name,
            cluster_credentials_secret=self.cluster_credentials_secret,
            query=sql_query,
            cluster_identifier=self.cluster_identifier

        )
        if not query_results:
            self.__logger.error(f"Error in executing stored procedure {proc_name}")
            return -1

        return query_results
