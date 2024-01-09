"""
Service: execute_sql
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
        :param kwargs:
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
            error_msg = None
            while status not in ("FINISHED", "FAILED"):
                response = self.__redshift.describe_statement(
                    Id=result.get("Id")
                )
                status = response.get("Status")
                error_msg = response.get('Error')

            if status == "FAILED":
                self.__logger.error(f"SQL query failed: {error_msg}")
                return None

        except Exception as exception:
            self.__logger.exception(f"Exception in running query: {exception}")
            return None

        return result.get("Id")
