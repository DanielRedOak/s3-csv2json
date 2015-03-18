__author__ = "Ryan O'Keeffe"

## This script will take AWS billing information file in an s3 bucket with specified prefix in the form of csv with a
## header and convert it to line based json for ingestion by things like Logstash.  Additionally, times will be
## optionally converted into the ISO8601 format that Logstash expects and a @timestamp field added. Files will be
## backed up to the same bucket under the 'backup_prefix' and converted files will be placed in the 'destination_prefix'
## Currently also handles zip files containing a single file, named the same as the archive sans the .zip extension

import boto
import csv
import json
import getopt
import sys
import re
import os
import zipfile
import datetime

source_bucket = ''
region_endpoint = 'us-east-1'
input_prefix = ''
output_prefix = ''
archive_prefix = ''
temp_dir = '/tmp/'
exclude_prefix = None

time_fields = ['UsageStartDate', 'InvoiceDate', 'UsageEndDate', 'BillingPeriodEndDate', 'BillingPeriodStartDate']
time_stamp_field = 'BillingPeriodEndDate'

def main(argv):

  global source_bucket
  global region_endpoint
  global input_prefix
  global output_prefix
  global archive_prefix
  global temp_dir
  global exclude_prefix
  global time_fields
  global time_stamp_field


  try:
    opts, args = getopt.getopt(argv, "s:i:o:a:r:e:")
  except getopt.GetoptError:
    print 's3-csv2json.py -s <source bucket> -i <input prefix> -o <output prefix> -a <archive prefix> -r <region | us-east-1 default> -e <exclude prefix>'
    sys.exit(2)
  for opt, arg in opts:
    if opt == '-h':
      print 's3-csv2json.py -s <source bucket> -i <input prefix> -o <output prefix> -a <archive prefix> -r <region | us-east-1 default> -e <exclude prefix>'
      sys.exit()
    elif opt in ("-s"):
      source_bucket = arg
    elif opt in ("-i"):
      input_prefix = arg
    elif opt in ("-o"):
      output_prefix = arg
    elif opt in ("-a"):
      archive_prefix = arg
    elif opt in ("-r"):
      region_endpoint = arg
    elif opt in ("-e"):
      exclude_prefix = arg

  if source_bucket == '' or output_prefix == '' or archive_prefix == '':
    print 'Missing required argument, usage: s3-csv2json.py -s <source bucket> -i <input prefix> -d <destination prefix> -b <archive_prefix> -r <region | us-east-1 default> -e <exclude prefix>'
    sys.exit(2)

  # Ensure we have our temp dir
  if not os.path.exists(temp_dir):
    os.makedirs(temp_dir)

  # Create the connection
  s3conn = boto.s3.connect_to_region(region_endpoint)

  # Get the bucket we want
  s3bucket = s3conn.get_bucket(source_bucket)

  # Do the work
  for existing_key in s3bucket.list(prefix=input_prefix):
    # Ignore folders, the output prefix, and the archive_prefix
    if not(existing_key.name.endswith('/') or existing_key.name.startswith(output_prefix) or existing_key.name.startswith(archive_prefix)) or (exclude_prefix != None and not existing_key.name.startswith(exclude_prefix)):
      print "Found key: " + existing_key.name
      regex = re.compile('^%s(.*)'%input_prefix)
      key_no_prefix = regex.match(existing_key.name).group(1)
      download_key(existing_key, temp_dir + key_no_prefix)
      processed_filename = process_file(temp_dir + key_no_prefix)
      newkey = s3bucket.new_key(output_prefix + key_no_prefix + ".json")
      upload_result(newkey, processed_filename)
      # Don't 'archive' unless we succeed in processing
      backup_key(existing_key, archive_prefix + key_no_prefix)
      os.remove(processed_filename)

def process_file(filename):
  print "Getting filehandles"
  if zipfile.is_zipfile(filename):
    ziphandle = open(filename, 'rb')
    zip = zipfile.ZipFile(ziphandle)
    # Usecase is our zip is one file named the same as our zip minus extension
    filename_clean = re.match('^(.*)\.zip$', os.path.basename(filename)).group(1)
    print filename_clean
    zipfh = zip.open(filename_clean)
    rv = process_csv(filename, zipfh)
    os.remove(filename)
    return rv
  else:
    rv = process_csv(filename, open(filename, 'r'))
    os.remove(filename)
    return rv

def process_csv(filename, filehandle):
  print "Processing contents of: " + filename
  jsonfile = open(filename + ".json", 'w')
  reader = csv.DictReader(filehandle)
  for row in reader:
    for key in time_fields:
      if key in row and row[key] != '':
        print "Checking:" + row[key]
        row[key] = convert_time(row[key])
    if time_stamp_field != '' and time_stamp_field in row and row[time_stamp_field] != '':
      row['@timestamp'] = row[time_stamp_field]
    json.dump(row, jsonfile)
    jsonfile.write('\n')
  return filename + ".json"

def upload_result(dest_key, local_filename):
  print "Outputting json to: " + dest_key.name
  dest_key.set_contents_from_filename(local_filename)


def backup_key(remote_key, dest_key):
  print "Backing up key:" + remote_key.name
  remote_key.copy(dst_bucket=source_bucket, dst_key=dest_key)
  remote_key.delete()

def download_key(remote_key, local_filename):
  remote_key.get_contents_to_filename(local_filename)

def convert_time(time):
  print "Performing time conversion:" + time
  return datetime.datetime.strptime(time, "%Y/%m/%d %H:%M:%S").isoformat(' ')

if __name__ == "__main__":
  main(sys.argv[1:])
