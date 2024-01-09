"""
Service: check_columns
Module: redshift_service
Author: Sourav Hazra
"""
import json
from copy import deepcopy

from helpers.redshift_helper import RedshiftHelper
from helpers.s3_helper import S3Helper


class RedshiftService:
    """
    Redshift Service for implementing business logic related to Redshift
    """

    def __init__(self, redshift, **dependencies):
        self.__s3 = dependencies.get("s3").get("resource")
        self.__s3_bucket_name = dependencies.get("s3").get("bucket_name")
        self.__s3_schema_key = dependencies.get("s3").get("s3_schema_key")
        self.__redshift = redshift
        self.__logger = dependencies.get("logger")
        self.__database_name = dependencies.get("redshift_params").get("database_name")
        self.cluster_identifier = dependencies.get("redshift_params").get("cluster_identifier")
        self.cluster_credentials_secret = dependencies.get("redshift_params").get("cluster_credentials_secret")

    def get_table_schema_from_definition(self):
        """
        Refer to the table schema stored in S3 Bucket and frame the create table stored procedure
        :return: [str, int]
        """
        # Get the table schema from S3
        schema = S3Helper(logger=self.__logger).fetch_object(
            s3=self.__s3,
            bucket_name=self.__s3_bucket_name,
            key=self.__s3_schema_key
        )

        if not schema:
            self.__logger.error(f"Error in getting schema using {self.__s3_schema_key} from {self.__s3_bucket_name}")
            return -1

        self.__logger.info("Schema fetch successful")

        try:
            schema = json.loads(schema)
        except Exception as excpetion:
            self.__logger.exception(f"Exception in parsing schema: {excpetion}")
            return None

        self.__logger.info("Schema parse successful")

        return schema

    def __add_column(self, database_name, schema_name, table_name, columns):
        """
        Add column to an existing table in Redshift
        :param database_name: String, schema_name: String, table_name: String, columns: List
        :return: [True, None]
        """
        try:
            for column_name, column_data_type in list(columns.items()):

                self.__logger.info(f"Adding column {column_name} {column_data_type} to {database_name}.{schema_name}.{table_name}")

                sql_query = f"""
                ALTER TABLE {database_name}.{schema_name}.{table_name} ADD COLUMN {column_name} {column_data_type};
                """

                redshift = RedshiftHelper(redshift=self.__redshift, logger=self.__logger)

                query_id = redshift.run_query(
                    database=self.__database_name,
                    cluster_credentials_secret=self.cluster_credentials_secret,
                    query=sql_query,
                    cluster_identifier=self.cluster_identifier
                )

                if not query_id:
                    self.__logger.error(f"Failed to add {column_name}")
                    return None

                self.__logger.info(f"{column_name} has been added")
        except Exception as exception:
            self.__logger.exception(f"Exception in adding columns: {exception}")
            return None
        return True

    def __drop_column(self, database_name, schema_name, table_name, columns):
        """
        Drop column to an existing table in Redshift
        :param database_name: String, schema_name: String, table_name: String, columns: Dict
        :return: [True, None]
        """
        try:
            for column_name in columns:
                self.__logger.info(f"Dropping column {column_name}")
                sql_query = f"""
                ALTER TABLE {database_name}.{schema_name}.{table_name} DROP COLUMN {column_name};
                """
                redshift = RedshiftHelper(redshift=self.__redshift, logger=self.__logger)

                query_id = redshift.run_query(
                    database=self.__database_name,
                    cluster_credentials_secret=self.cluster_credentials_secret,
                    query=sql_query,
                    cluster_identifier=self.cluster_identifier
                )

                if not query_id:
                    self.__logger.error(f"Failed to drop {column_name}")
                    return None

                self.__logger.info(f"{column_name} has been dropped")
        except Exception as exception:
            self.__logger.exception(f"Exception in dropping columns: {exception}")
            return None
        return True

    def make_table_consistent_with_definition(self, database_name, schema_name, table_name, schema):
        """
        Make an existing table in Redshift consistent with the Redshift schema definition
        :param database_name: String, schema_name: String, table_name: String, schema: Dict
        :return: [True, None]
        """
        self.__logger.info(f"Making table {database_name}.{schema_name}.{table_name} consistent")
        sql_query = f"""
        select * from pg_get_cols('{database_name}.{schema_name}.{table_name}')
        cols(view_schema name, view_name name, col_name name, col_type varchar, col_num int);
        """
        redshift = RedshiftHelper(redshift=self.__redshift, logger=self.__logger)

        query_id = redshift.run_query(
            database=self.__database_name,
            cluster_credentials_secret=self.cluster_credentials_secret,
            query=sql_query,
            cluster_identifier=self.cluster_identifier
        )

        if not query_id:
            return -1

        columns = redshift.get_query_results(query_id=query_id)
        column_definition = deepcopy(schema.get("columns"))
        columns_to_drop = []
        for column in columns:
            column_name = list(column[2].values())[0]
            if column_name == "migration_type":
                continue
            if not column_definition.get(column_name):
                self.__logger.info(f"Column name {column_name} needs to be deleted")
                columns_to_drop.append(column_name)
            else:
                column_definition.pop(column_name)

        if not column_definition and not columns_to_drop:
            self.__logger.info("No columns modified since last check")
        else:
            self.__logger.info(f"{','.join(list(column_definition.keys()))} columns need(s) to be added")
            add_column = self.__add_column(database_name, schema_name, table_name, column_definition)
            drop_column = self.__drop_column(database_name, schema_name, table_name, columns_to_drop)
            if not add_column or not drop_column:
                return -2
        return True
