__author__ = "Ryan O'Keeffe"

## This script will take AWS billing information file in an s3 bucket with specified prefix in the form of csv with a
## header and convert it to line based json for ingestion by things like Logstash.  Files will be backed up to the same
##  bucket under the 'backup_prefix' and converted files will be placed in the 'destination_prefix'

import boto
import csv
import json
import getopt
import sys

def main(argv):
  source_bucket = ''
  region_endpoint = 'us-east-1'
  input_prefix = ''
  output_prefix = ''
  backup_prefix = ''

  try:
    opts, args = getopt.getopt(argv, "s:i:o:b:r:")
  except getopt.GetoptError:
    print 's3-csv2json.py -s <source bucket> -i <input prefix> -o <output prefix> -b <backup prefix> -r <region | us-east-1 default>'
    sys.exit(2)
  for opt, arg in opts:
    if opt == '-h':
      print 's3-csv2json.py -s <source bucket> -i <input prefix> -o <output prefix> -b <backup prefix> -r <region | us-east-1 default>'
      sys.exit()
    elif opt in ("-s"):
      source_bucket = arg
    elif opt in ("-i"):
      input_prefix = arg
    elif opt in ("-o"):
      output_prefix = arg
    elif opt in ("-b"):
      backup_prefix = arg
    elif opt in ("-r"):
      region_endpoint = arg

  if source_bucket == '' or input_prefix == '' or output_prefix == '' or backup_prefix == '':
    print 'Missing required argument, usage: s3-csv2json.py -s <source bucket> -i <input prefix> -d <destination prefix> -b <backup prefix> -r <region | us-east-1 default>'
    sys.exit(2)

  # Create the connection
  s3conn = boto.s3.connect_to_region(region_endpoint)

  # Get the bucket we want
  s3bucket = s3conn.get_bucket(source_bucket)

  # Do the work
  for existing_key in s3bucket.list(prefix=input_prefix):
    if not(existing_key.name.endswith('/')):
      print "Found key: " + existing_key.name
      if "/" in existing_key.name:
        prefix, key_name = existing_key.name.split("/",1)
        filename, extension = key_name.split(".",1)
        newkey = s3bucket.new_key(output_prefix + filename + ".json")
        print "Processing contents of: " + existing_key.name
        key_contents= existing_key.get_contents_as_string()
        key_csv = csv.DictReader(key_contents.splitlines(), delimiter=',', quotechar='"')
        key_json = ''
        for row in key_csv:
          key_json+=json.dumps(row)
          key_json+='\n'
        print "Backing up key:" + existing_key.name
        existing_key.copy(dst_bucket=source_bucket, dst_key=backup_prefix + key_name)
        existing_key.delete()
        print "Outputting json to: " + newkey.name
        newkey.set_contents_from_string(key_json)

if __name__ == "__main__":
  main(sys.argv[1:])