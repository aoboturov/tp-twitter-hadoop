from boto.s3.connection import S3Connection
from boto.s3.key import Key
import os

# TODO: provide real AWS credentials
aws_access_key = ''
aws_secret_key = ''

conn = S3Connection(aws_access_key, aws_secret_key)

# TODO: provide the bucket name
bucket = conn.create_bucket('bucket_name')

# TODO: change to real file location on disk
file_name = 'on_disk_file_location'
file_size = os.stat(file_name).st_size
file_pt = file(file_name)

# TODO: change s3 Key name
k = Key(bucket, 's3_key_name')
md5sum = k.compute_md5(file_pt)

print 'md5 sum:'
print md5sum

written_size = k.set_contents_from_filename(file_name, md5=md5sum)

print 'actual size:'
print file_size
print 'written size:'
print written_size
print 'are they equal?'
print (file_size == written_size)
