"""
Service: etl_invoker
Module: s3_helper
Author: Sourav Hazra
"""


class S3Helper:
    """
    S3 Helper to perform S3 operations
    """
    def __init__(self, **kwargs):
        """
        Constructor for S3Helper
        :param kwargs:
        :return:
        """
        self.__logger = kwargs.get("logger")

    def list_objects(self, s3, bucket_name):
        """
        List objects under S3 Bucket
        :param s3: S3 resource
        :param bucket_name: str
        :return: [List, None]
        """
        objects = []
        try:
            bucket = s3.Bucket(bucket_name)
            for obj in bucket.objects.all():
                objects.append(obj.key)
        except Exception as exception:
            self.__logger.exception(
                f"Exception in getting list of objects under {bucket_name}: {exception}"
            )
            return None
        return objects
