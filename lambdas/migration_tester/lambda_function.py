from helpers.redshift_helper import RedshiftHelper
import psycopg2
import boto3
import json
import os
from aws_lambda_powertools.utilities import parameters

client_redshift = boto3.client("redshift-data")
redshift = RedshiftHelper(redshift=client_redshift)

s3 = boto3.resource("s3")
objects = []


def lambda_handler(event, context):
    bucket = s3.Bucket(os.getenv("S3_MIGRATION_BUCKET"))
    for object in bucket.objects.all():
        objects.append(object.key)

    db_credentials = json.loads(parameters.get_secret(os.getenv("AURORA_CREDENTIALS")))

    databases = list(filter(lambda x: x.startswith("ddl/tables"), objects))

    for database in databases:
        s3_object = s3.Object(os.getenv("S3_MIGRATION_BUCKET"), database)
        tables = json.loads(s3_object.get().get("Body").read().decode('utf-8'))
        connection = psycopg2.connect(
            f"host={os.getenv('DB_HOST_NAME')} "
            f"dbname={database.split('/')[-1].split('.')[0]} "
            f"user={db_credentials.get('userName')} "
            f"password={db_credentials.get('password')}")
        cursor = connection.cursor()
        for table in tables:
            sql_query = f"SELECT COUNT(*) FROM {table.get('databaseName')}.{table.get('tableName')}"
            # sql_query = f"DROP TABLE {table.get('databaseName')}.{table.get('schemaName')}.{table.get('tableName')} CASCADE"
            query_id = redshift.run_query(
                database=os.getenv("REDSHIFT_DATABASE_NAME"),
                cluster_credentials_secret=os.getenv("CLUSTER_CREDENTIALS"),
                query=sql_query,
                cluster_identifier=os.getenv("CLUSTER_IDENTIFIER")
            )
            cursor.execute(f"SELECT COUNT(*) FROM {table.get('tableName')}")
            print(
                table.get('databaseName'),
                table.get('tableName'),
                redshift.get_query_results(query_id=query_id)[0][0].get("longValue"),
                cursor.fetchall()[0][0]
            )
        connection.close()


