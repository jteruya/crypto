import boto3

class s3_api():

    def __init__(self):
        self.s3_conn = None

    def connect(self):
        """
        Set object attribute s3_conn to a live S3 connection object.
        :return: None
        """
        self.s3_conn = boto3.resource('s3')

    def upload_file(self, bucket_name, output_file_path):
        """
        Upload a file given the absolute path of the local file and bucket name
        :param bucket_name: S3 bucket name
        :param output_file_path: absolute path of the local file
        :return: None
        """
        if self.s3_conn is not None:
            file_name = output_file_path.split("/")[-1]
            self.s3_conn.meta.client.upload_file(output_file_path, bucket_name, file_name)
