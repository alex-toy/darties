import logging
import boto3
from botocore.exceptions import ClientError

import app.config.config as cf

config = configparser.ConfigParser()
config.read_file(open(cf.config_file))

aws_access_key_id = config.get('default','aws_access_key_id')
aws_secret_access_key = config.get('default','aws_secret_access_key')


def create_bucket(bucket_name, region=None):
    """
    Create an S3 bucket in a specified region

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :param bucket_name: Bucket to create
    :param region: String region to create bucket in, e.g., 'us-west-2'
    :return: True if bucket created, else False
    """

    try: # Create bucket
        if region is None:
            s3_client = boto3.client(
                's3',
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
            )
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client = boto3.client(
                's3',
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region_name=region
            )
            location = {'LocationConstraint': region}
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration=location
            )
    except ClientError as e:
        logging.error(e)
        return False
    return True



if __name__ == '__main__':
    bucket_name = "darties"
    region="us-west-2"
    create_bucket(bucket_name, region)
    file_name = cf.OUTPUTS_FILE
    upload_file(file_name, bucket_name, object_name=None)
