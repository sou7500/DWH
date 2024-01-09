"""
Service: create_table
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
            error_msg = None
            while status not in ("FINISHED", "FAILED"):
                response = self.__redshift.describe_statement(
                    Id=result.get("Id")
                )
                status = response.get("Status")
                error_msg = response.get('Error')

            if status == "FAILED":
                self.__logger.error(f"SQL statement failed: {error_msg}")
                return None

        except Exception as exception:
            self.__logger.exception(f"Exception in executing SQL statement: {exception}")
            return None
        return True
