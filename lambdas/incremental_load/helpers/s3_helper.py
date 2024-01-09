"""
Service: incremental_load
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

    def fetch_object(self, s3, bucket_name, key):
        """
        Fetch the contents of an object from a given S3 bucket with the specified key
        """
        try:
            s3_object = s3.Object(bucket_name, key)
            return s3_object.get().get("Body").read().decode('utf-8')
        except Exception as exception:
            self.__logger.exception(f"Exception in fetching {key} from {bucket_name}: {exception}")
            return None
