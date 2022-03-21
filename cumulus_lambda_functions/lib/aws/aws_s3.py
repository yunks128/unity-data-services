from io import BytesIO

import boto3


class AwsS3:
    def __init__(self):
        self.__s3_resource = boto3.Session().resource('s3')
        self.__s3_client = boto3.Session().client('s3')
        self.__target_bucket = None
        self.__target_key = None

    @property
    def target_bucket(self):
        return self.__target_bucket

    @target_bucket.setter
    def target_bucket(self, val):
        """
        :param val:
        :return: None
        """
        self.__target_bucket = val
        return

    @property
    def target_key(self):
        return self.__target_key

    @target_key.setter
    def target_key(self, val):
        """
        :param val:
        :return: None
        """
        self.__target_key = val
        return

    def get_stream(self):
        if self.target_bucket is None or self.target_key is None:
            raise ValueError('bucket or key is None. Set them before calling this method')
        return self.__s3_client.get_object(Bucket=self.target_bucket, Key=self.target_key)['Body']

    def upload_bytes(self, content: bytes):
        self.__s3_client.put_object(Bucket=self.target_bucket,
                                    Key=self.target_key,
                                    ContentType='binary/octet-stream',
                                    Body=content,
                                    ServerSideEncryption='AES256')
        return

    def read_small_txt_file(self):
        """
        convenient method to read small text files stored in S3

        :param bucket: bucket name
        :param key: S3 key
        :return: text file contents
        """
        bytestream = BytesIO(self.get_stream().read())  # get the bytes stream of zipped file
        return bytestream.read().decode('UTF-8')
