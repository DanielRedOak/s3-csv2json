__author__ = "Ryan O'Keeffe"

## This script will take AWS billing information file in an s3 bucket with specified prefix in the form of csv with a
## header and convert it to line based json for ingestion by things like Logstash.  Files will be backed up to the same
##  bucket under the 'backup_prefix' and converted files will be placed in the 'destination_prefix'


import boto
import csv
import json

source_bucket = 'rdo-test'
region_endpoint = 'us-east-1'
source_prefix = 'input/'
destination_prefix = 'output/'
backup_prefix = 'backup/'


# Create the connection
s3conn = boto.s3.connect_to_region(region_endpoint)

# Get the bucket we want
s3bucket = s3conn.get_bucket(source_bucket)

# Do the work
for existing_key in s3bucket.list(prefix=source_prefix):
  if not(existing_key.name.endswith('/')):
    if "/" in existing_key.name:
      prefix, key_name = existing_key.name.split("/",1)
      filename, extension = key_name.split(".",1)
      newkey = s3bucket.new_key(destination_prefix + filename + ".json")
      key_contents= existing_key.get_contents_as_string()
      key_csv = csv.DictReader(key_contents.splitlines(), delimiter=',', quotechar='"')
      key_json = ''
      for row in key_csv:
        key_json+=json.dumps(row)
        key_json+='\n'
      existing_key.copy(dst_bucket=source_bucket, dst_key=backup_prefix + key_name)
      existing_key.delete()
      newkey.set_contents_from_string(key_json)


