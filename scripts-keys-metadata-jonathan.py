import os
import json
import subprocess
import sys
from boto import dynamodb2
from boto.dynamodb2.table import Table
from boto.dynamodb2.items import Item
import commands
from boto.s3.connection import S3Connection
import boto
from boto.s3.key import Key
from collections import OrderedDict

AWS_KEY = '<AWS_KEY>'
AWS_SECRET = '<AWS_SECRET>'
TABLE_NAME1 = "<dynamo table name>"
REGION = "<aws region>"

conn_to_s3 = S3Connection(AWS_KEY,AWS_SECRET, host='s3.amazonaws.com')
bucket = conn_to_s3.get_bucket('<bucket name>')


conn_to_dynamo = dynamodb2.connect_to_region(
        REGION, 
        aws_access_key_id = AWS_KEY,
        aws_secret_access_key= AWS_SECRET
)
table_s3_metadata = Table(
        TABLE_NAME1, 
        connection=conn_to_dynamo
)


def searchFiles(path, searchTerm):
    counter = 0
    for key in bucket.get_all_keys(prefix=path, delimiter='/'):
        counter += 1
    if counter < 2:
        return
    else:
        for key in bucket.get_all_keys(prefix=path, delimiter='/'):
            if key.name == searchTerm:
                pass
            elif key.name.endswith('/'): 
                searchFiles(key.name, key.name)
            else:
                print(key.name.encode('utf-8'))
                saveToDynamo(key.name.encode('utf-8'))
        return


def saveToDynamo(filename):
    d = {}
    try:
        d = getSingleFileMetadata(filename)
    except:
        pass
    
    if len(d) > 0:
        newItem = Item(table_s3_metadata, data=d)
        newItem.save(overwrite=True)


def getSingleFileMetadata(file):
    d = {}
    singleMetadata = commands.getstatusoutput("aws s3api head-object --bucket edlstage0-datastore --key " + file)
    json_object = json.loads(singleMetadata[1]) #its a tuple with second element being metadata, first is result
    
    d['filename'] = file
    


    for key in json_object:
        if key != 'Metadata':
            d[key]=json_object[key]
        if key == 'Metadata':
            for k in json_object['Metadata']:
                d[k] = json_object['Metadata'][k]
    

    return d

def main():
    #point at prefix - it will list every file contained within (recursively)
    searchTerm0 = '<path-to-directory1>'
    searchTerm1 = '<path-to-directory2>'
    searchTerm2 = '<path-to-directory3>'
    # etc...


    searchFiles(searchTerm0, searchTerm0)
    searchFiles(searchTerm1, searchTerm1)
    searchFiles(searchTerm2, searchTerm2)    
    

if __name__ == "__main__":main()
