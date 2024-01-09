"""
Service: execute_sql
Module: redshift_service
Author: Sourav Hazra
"""
from helpers.redshift_helper import RedshiftHelper
from helpers.s3_helper import S3Helper


class RedshiftService:
    """
    Redshift Service for implementing business logic related to Redshift
    """
    def __init__(self, redshift, **dependencies):
        self.__s3 = dependencies.get("s3").get("resource")
        self.__s3_bucket_name = dependencies.get("s3").get("bucket_name")
        self.__s3_sql_key = dependencies.get("s3").get("s3_sql_key")
        self.__redshift = redshift
        self.__logger = dependencies.get("logger")
        self.__database_name = dependencies.get("redshift_params").get("database_name")
        self.cluster_identifier = dependencies.get("redshift_params").get("cluster_identifier")
        self.cluster_credentials_secret = dependencies.get("redshift_params").get("cluster_credentials_secret")

    def __fetch_sql_statement(self):
        """
        Fetch SQL statement from S3
        :return: [None, str]
        """

        self.__logger.info(f"Fetching the SQL statement in {self.__s3_sql_key} from {self.__s3_bucket_name}")
        statement = S3Helper(logger=self.__logger).fetch_object(
            s3=self.__s3,
            bucket_name=self.__s3_bucket_name,
            key=self.__s3_sql_key
        )

        if not statement:
            self.__logger.error(
                f"Error in Fetching the SQL statement in {self.__s3_sql_key} from {self.__s3_bucket_name}"
            )
            return None

        self.__logger.info(f"SQL statement to run: {statement}")

        return statement

    def __run_sql_query(self, sql_query):
        """
        Run SQL query in Redshift
        :param sql_query: str
        :return: str
        """

        # Run the SQL query to invoke the stored procedure
        self.__logger.info("Running SQL statement")
        query_results = RedshiftHelper(redshift=self.__redshift, logger=self.__logger).run_query(
            database=self.__database_name,
            cluster_credentials_secret=self.cluster_credentials_secret,
            query=sql_query,
            cluster_identifier=self.cluster_identifier
        )

        return query_results

    def __create_stored_procedure(self):
        """
        Create stored procedure in Redshift
        :return: [str, None]
        """
        try:
            self.__logger.info("Getting stored procedure")
            stored_procedure = self.__fetch_sql_statement()

            if not stored_procedure:
                self.__logger.error("Error in getting stored procedure")
                return None

            query_results = self.__run_sql_query(
                sql_query=stored_procedure
            )
            if not query_results:
                self.__logger.error("Error encountered while creating stored procedure")
                return None

            proc_name = stored_procedure.split(" ")[4][:stored_procedure.split(" ")[4].index('(')].strip()
            self.__logger.info(f"{proc_name} has been created")
        except Exception as exception:
            self.__logger.exception(f"Error in creating stored procedure: {exception}")
            return None
        return proc_name

    def __invoke_stored_procedure(self, *proc_args):
        """
        Invoke stored procedure create in Redshift
        :param proc_args: Dict
        :return: [str, -1, -2]
        """
        proc_name = self.__create_stored_procedure()
        if not proc_name:
            return -1

        self.__logger.info(f"Invoking stored procedure {proc_name}")

        sp_args_list = list(map(lambda x: f"'{x}'", proc_args))

        # Frame SQL query to invoke the stored procedure
        sql_query = f"CALL {proc_name}({','.join(sp_args_list)});"

        query_result = self.__run_sql_query(sql_query=sql_query)

        if not query_result:
            self.__logger.error(f"Error encountered while invoking stored procedure: {sql_query}")
            return -2

        return query_result

    def run_sql_statement(self, **sql_query_args):
        """
        Run SQL statement or stored procedure in Redshift
        :param sql_query_args: Dict
        :return: [str, -1, -2]
        """
        self.__logger.info(f"SQL query type: {sql_query_args.get('type')}")
        if sql_query_args.get("type") == "PROC":
            query_results = self.__invoke_stored_procedure(
                *sql_query_args.get("proc_arguments")
            )
        elif sql_query_args.get("type") == "ADHOC":
            query_results = self.__run_sql_query(
                sql_query=sql_query_args.get("query")
            )
            if not query_results:
                self.__logger.error("Error in running ad-hoc query")
                return -2
        else:
            sql_statement = self.__fetch_sql_statement()
            if not sql_statement:
                return -1
            query_results = self.__run_sql_query(
                sql_query=sql_statement
            )
            if not query_results:
                self.__logger.error("Error in running SQL query")
                return -2
        return query_results
