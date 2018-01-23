from yaml import load as yaml_load
from botocore.errorfactory import ClientError
import os
import re
import boto3
import requests
import celeryconfig

app = Celery()
app.config_from_object(celeryconfig)


def s3_source_exists(bagname):
    """ Check that source bag exists in S3 """
    s3_bucket = 'ul-bagit'
    s3 = boto3.resource('s3')
    s3_key = "{0}/{1}/{3}".format('source', bag, 'bag-info.txt')
    try:
        s3.head_object(Bucket=s3_bucket, Key=s3_key)
        return True
    except ClientError:
        return False


def yield_s3_source_images(bagname):
    """ return iterator of source images as s3 keys (strings) """
    s3_bucket = 'ul-bagit'
    extensions = ['tif', 'tiff']
    ignore_files = ['_orig$']  # ignore if file name contains regex
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(s3_bucket)
    source_location = "source/{0}/data".format(bagname)
    for obj in bucket.objects.filter(Prefix=source_location):
        if obj.key.split('.')[-1].lower() in extensions:  
            if not any(map(lambda x: re.search(x, obj.key.split('.')[-2]), ignore_files)):
                yield obj.key


def get_mmsid_from_name(bagname):
     """ get the mmsid from end of bag name """
    mmsid = bag.split("_")[-1].strip()  # using bag name formatting: 1990_somebag_0987654321
    if re.match("^[0-9]+$", mmsid):  # check that we have an mmsid like value
        return mmsid
    return None


def get_mmsid_from_s3(bagname):
    """ get the mmsid from bag-info.txt in s3 """
    s3_bucket = 'ul-bagit'
    s3 = boto3.resource('s3')
    s3_key = "{0}/{1}/{2}".format('source', bagname, 'bag-info.txt')
    recipe_obj = s3.Object(s3_bucket, s3_key)
    bag_info = yaml_load(recipe_obj.get()['Body'].read())
    try:
        mmsid = bag_info['FIELD_EXTERNAL_DESCRIPTION'].split()[-1].strip()
    except KeyError:
        print("Cannot determine mmsid for bag: {0}".format(bag))
        return None
    if re.match("^[0-9]+$", mmsid):  # check that we have an mmsid like value
        return mmsid
    return None


def s3_object_file_size(s3_key):
    """ get size of object in bytes  """
    s3 = boto3.client('s3')
    try:
        response = s3.head_object(Bucket='ul-bagit', Key=s3_key)
        return response['ContentLength']
    except ClientError:
        return None


def available_disk_space(path):
    """ get available disk space in bytes """
    stats = os.statvfs(path)
    return stats.f_bsize * stats.f_bavai


def get_collection(auth_token, mmsid):
    """ guess collection based on alma record details"""
    url = "https://api-na.hosted.exlibrisgroup.com/almaws/v1/bibs/{0}/holdings/?apikey={1}"
    response = requests.get(url.format(mmsid, auth_token))
    xml = etree.fromstring(response.content.strip())
    # TODO: Add missing collections' namespace:PID
    collections = {
        'oku:bzb': '//holdings/holding/library[text()="BIZZELL"] and //holdings/holding/location[text()="BIZZ_BIBLE"]',
        'oku:bbh': '//holdings/holding/library[text()="BIZZELL"] and //holdings/holding/location[text()="BASS_COLL"]',
        'oku:whc': '//holdings/holding/library[text()="WESTRNHIST"]',
        'oku:hos': '//holdings/holding/library[text()="HISTSCI"]',
    }
    for collection, path in collections.items():
        if xml.xpath(path):
            return collection


def search_mongodb(filter_by, count=0):
    """ list bags in mongodb by filter """
    db_client = app.backend.database.client
    digital_objects = db_client.catalog.digital_objects
    missing_derivatives = digital_objects.find(filter_by,{'bag':1})
    return [x['bag'] for x in list(missing_derivatives.limit(count))]


