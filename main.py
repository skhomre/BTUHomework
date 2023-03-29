import boto3
from os import getenv 
from dotenv import load_dotenv
import logging
from botocore.exceptions import ClientError
import argparse
import json
import time
import sys


load_dotenv()
def init_client():
  try:
    client = boto3.client("s3", aws_access_key_id=getenv("aws_access_key_id"),aws_secret_access_key=getenv("aws_secret_access_key"),aws_session_token=getenv("aws_session_token"))
    return client
  except ClientError as e:
    logging.error(e)




def create_bucket(client, name, region = "us-east-2"):
  try:
    location = {"LocationConstraint" : region}
    client.create_bucket(Bucket = name,CreateBucketConfiguration = location )
    return True
  except Exception as e:
    print(e)
    return False


def delete_bucket(s3,name):
  try:
    s3.delete_bucket(Bucket=name)
  except Exception as e:
    return e
  return True


def bucket_exists(s3,bucket_name):
    exists = "Bucket Already Exists"
    try:
        s3.head_bucket(Bucket=bucket_name)
    except ClientError as e:
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            exists = "Not Exist"  
            return exists 
        return e
    return exists

def create_bucket_policy(s3,bucket_name,jsonfile):
  try:
    policy = json.load(open(jsonfile)) 
    s3.put_bucket_policy(Bucket=bucket_name, Policy=json.dumps(policy))
  except Exception as e:
    return e
  return True

def read_bucket_policy(s3,bucket_name):
  try:
    response = s3.get_bucket_policy(Bucket=bucket_name)
    return response['Policy']
  except Exception as e:
    return e

def list_buckets(s3):
  return s3_client.list_buckets()["Buckets"]

def generate_public_read(s3,bucket_name):
  bucket_policy = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "PublicReadGetObject",
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": f"arn:aws:s3:::{bucket_name}/*"
        }
    ]
} 
  return json.dumps(bucket_policy, indent=4)

def put_object_acl(s3,bucket_name,object_name,jsonfile):
  try:
    policy = json.load(open(jsonfile))
    response = s3.put_object_acl(
    ACL='public-read',
    Bucket=bucket_name,
    Key=object_key,
    AccessControlPolicy={
        'Policy': json.dumps(policy)
    }
) 
    return response
  except Exception as e:
    return e


def download_file(s3,bucket_name,object_path,save_path):

  try:
    s3.Bucket(bucket_name).download_file(object_path, save_path)
    return f"File downloaded from S3 bucket '{bucket_name}' to '{save_path}'"
  except Exception as e:
    return e
  return "Unsupported Format"

def upload_file(s3,bucket_name,file,object_path):
  obj = magic.Magic()
  ftype = obj.from_file(file)
  supported_formats = (".bmp, .jpg, .jpeg, .png, .webp, .mp4")
  for i in supported_formats:
    if i in ftype.lower():
      try:
        s3.upload_file(file, bucket_name, object_path)
        return f"File uploaded to S3 bucket '{bucket_name}' to '{object_path}'"
      except Exception as e:
        return e
  return "Unsupported Format"

def large_upload(s3,bucket_name,file,part_size=10):
  each_size = part_size * 1024 * 1024
  multipart_upload = s3.meta.client.create_multipart_upload(
    Bucket=bucket_name,
    Key=file
)
  with open(file, 'rb') as file:
    parts = []
    part_number = 1
    while True:
        data = file.read(each_size)
        if not data:
            break
        response = s3.meta.client.upload_part(
            Bucket=bucket_name,
            Key=file,
            PartNumber=part_number,
            UploadId=multipart_upload['UploadId'],
            Body=data
        )
        parts.append({
            'PartNumber': part_number,
            'ETag': response['ETag']
        })
        part_number += 1

  s3.meta.client.complete_multipart_upload(
  Bucket=bucket_name,
  Key=file,
  UploadId=multipart_upload['UploadId'],
  MultipartUpload={'Parts': parts}
)
    return True

def bucket_action(s3,bucket_name,key,flag):
  obj = s3.Object(bucket_name, key)
  if flag == 'del':
    obj.delete()
    return f"{key} was deleted from {bucket_name}"
  elif flag == 'copy':
    obj.copy_from(CopySource={'Bucket': bucket_name, 'Key': key})
    return f"{key} copied"
  elif flag == 'rename':
    new_key = input("type new name ")
    s3.Object(bucket_name, new_key).copy_from(CopySource={'Bucket': bucket_name, 'Key': key})
    
    # Delete the object with the old key name
    s3.Object(bucket_name,key).delete()
    
    return f"{key} renamed to {new_key} in bucket {bucket_name}"




if __name__ == "__main__":
  s3_client = init_client()
  function_names = [name for name in dir() if callable(globals()[name])]
  for i in function_names:
    if "ClientError" in i or "init" in i or "env" in i:
      function_names.remove(i)
  parser = argparse.ArgumentParser(description = f"you can use following methods: {function_names}")
  parser.add_argument('command', type=str, nargs='+')
  args = parser.parse_args()
  try:
    todo = globals()[args.command[0]]
  except KeyError:
    print("Unknown Command, Use Help For Available Commands")
    sys.exit()


  todo = globals()[args.command[0]]
  print(todo(s3_client,*args.command[1:]))



