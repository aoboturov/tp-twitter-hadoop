from boto.s3.connection import S3Connection
from boto.s3.key import Key
import os

# TODO: provide real AWS credentials
aws_access_key = ''
aws_secret_key = ''

conn = S3Connection(aws_access_key, aws_secret_key)

# TODO: provide the bucket name
bucket = conn.create_bucket('bucket_name')

# TODO: change to real file name on disk
file_name = 'on_disk_file_location'

# TODO: change s3 Key name
k = Key(bucket, 's3_key_name')

k.get_contents_to_filename(file_name)

