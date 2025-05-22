import os
import json
import boto3

# AWS S3 variables and objects
s3_bucket = 'vwd-test-bucket'
session = boto3.Session(profile_name = 'boto3-admin')
s3 = session.client('s3')

# Local data variables and objects
s3_asset_folder_name = 's3_assets'
json_file = s3_asset_folder_name + '/' + 'downloaded_files.json'

# Loading in data from json_file
try:
    with open(json_file) as file:
        json_data = json.load(file)
except Exception as e:
    raw_data = '{}'
    json_data = json.loads(raw_data)

# Checking if file detailed in json_data exist in s3_asset_folder_name
missing_files = []
for key, value in json_data.items():
    if os.path.exists(str(s3_asset_folder_name) + str(key)):
        print(str(key) + ' exists!')
    else:
        missing_files.append(key)
        print(str(key) + ' does not exist...')

#
for key_to_remove in missing_files:
    # checking if the key exists before removing
    if key_to_remove in json_data:
        removed_value = json_data.pop(key_to_remove)
        print(f"Removed key '{key_to_remove}' with value: {removed_value}")

#TODO need to update program to download files if json_data is empty
if not json_data:
    print('No files have been downloaded')
else:
    print('Files exist.')

# Iterating through all the objects in s3_bucket
for key in s3.list_objects(Bucket = s3_bucket)['Contents']:
    # Listing all versions of an object
    versions = s3.list_object_versions(Bucket = s3_bucket, Prefix = key['Key'])
    for version in versions.get('Versions', []):
        if version['IsLatest']:
            for key, value in json_data.items():
                if version['Key'] == key:
                    print('This file: ' + str(key) + ' has been downloaded before.')
                    if version['VersionId'] == value:
                        print('We have the latest version of this file: ' + str(value) + ', no need to download.')
                        break
                    else:
                        print('Local file: ' + str(key) + ' is out of data, re-downloading...')
                        #s3.download_file(s3_bucket, s3_file, s3_asset_folder_name + s3_file)
                else:
                    print('This file has not been encountered before. Downloading...')
                    #s3.download_file(s3_bucket, s3_file, s3_asset_folder_name + s3_file)
                json_field = {version['Key'] : version['VersionId']}
                json_data.update(json_field)
                print('Updating ' + str(json_file) + ' with value: ' + str(json_field))

# Saving json_data to json_file
with open(json_file, 'w') as outfile:
    outfile.write(json.dumps(json_data))