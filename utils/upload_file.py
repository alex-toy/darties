import logging
import boto3
import os
from botocore.exceptions import ClientError
import configparser

import app.config.config as cf

config = configparser.ConfigParser()
config.read_file(open(cf.config_file))

aws_access_key_id = config.get('S3_USER','aws_access_key_id')
aws_secret_access_key = config.get('S3_USER','aws_secret_access_key')


def upload_file(file_name, year, bucket, object_name=None, ACL={'ACL' : 'public-read'}):
    """
    Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    object_name = os.path.join(year, cf.SAVED_FILENAME)

    # Upload the file
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )
    try:
        response = s3_client.upload_file(file_name, bucket, object_name, ExtraArgs=ACL)
    except ClientError as e:
        logging.error(e)
        return False
    return True


if __name__ == '__main__':
    bucket_name = "darties"

    year = '2020'
    file_name = os.path.join(cf.OUTPUTS_DIR, year, cf.SAVED_FILENAME)

    upload_file(file_name, year, bucket_name, object_name)
