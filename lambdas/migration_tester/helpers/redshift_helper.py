class RedshiftHelper:
    def __init__(self, redshift):
        self.__redshift = redshift

    def run_query(self, **kwargs):
        """
        Run a SQL query in Redshift
        :param kwargs:
        :return:
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
                print("SQL query failed")
                return

        except Exception as exception:
            print(exception)
        else:
            return result.get("Id")

    def get_query_results(self, query_id):
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
                print(f"Error in getting query results: {exception}")
            else:
                return result
