"""
Service: check_columns
Module: redshift_helper
Author: Sourav Hazra
"""


class RedshiftHelper:
    """
    Redshift Helper for Redshift operations
    """

    def __init__(self, **kwargs):
        """
        Constructor method for RedshiftHelper
        :param kwargs: Dict
        """
        self.__redshift = kwargs.get("redshift")
        self.__logger = kwargs.get("logger")

    def run_query(self, **kwargs):
        """
        Run a SQL query in Redshift
        :param kwargs: Dict
        :return: [None, String]
        """
        try:
            result = self.__redshift.execute_statement(
                Database=kwargs.get("database"),
                SecretArn=kwargs.get("cluster_credentials_secret"),
                Sql=kwargs.get("query"),
                ClusterIdentifier=kwargs.get("cluster_identifier")
            )

            status = "START"

            while status not in ("FINISHED", "FAILED"):
                response = self.__redshift.describe_statement(
                    Id=result.get("Id")
                )
                status = response.get("Status")

            if status == "FAILED":
                self.__logger.error("SQL query failed")
                return None

        except Exception as exception:
            self.__logger.exception(f"Exception in running query: {exception}")
            return None
        return result.get("Id")

    def get_query_results(self, query_id):
        """
        Get query results after running a query in Redshift
        :param query_id: String
        :return: [None, String]
        """
        next_token = 1
        result = []

        while next_token:
            try:
                if next_token == 1:
                    response = self.__redshift.get_statement_result(
                        Id=query_id
                    )
                else:
                    response = self.__redshift.get_statement_result(
                        Id=query_id,
                        NextToken=next_token
                    )
                result += response.get("Records")
                next_token = response.get("NextToken")
            except Exception as exception:
                self.__logger.exception(f"Error in getting query results: {exception}")
                return None
            return result
