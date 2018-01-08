from celery.task import task
from yaml import load as yaml_load
from botocore.errorfactory import ClientError
import boto3
import logging
import requests
import celeryconfig

app = Celery()
app.config_from_object(celeryconfig)


def s3_source_exists(bagname):
    """ Check that source bag exists in S3 """
    s3_bucket='ul-bagit'
    s3 = boto3.resource('s3')
    s3_key = "{0}/{1}/{3}".format('source', bag, 'bag-info.txt')
    try:
        s3.head_object(Bucket=s3_bucket, Key=s3_key)
        return True
    except ClientError:
        return False


def get_mmsid_from_name(bagname):
     """ get the mmsid from end of bag name """
    mmsid = bag.split("_")[-1].strip()  # using bag name formatting: 1990_somebag_0987654321
    if re.match("^[0-9]+$", mmsid):  # check that we have an mmsid like value
        return mmsid
    return None


def get_mmsid_from_s3(bagname):
    """ get the mmsid from bag-info.txt in s3 """
    s3_bucket='ul-bagit'
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


def get_collection(token, mmsid):
    """ guess collection based on alma record details"""
    url = "https://api-na.hosted.exlibrisgroup.com/almaws/v1/bibs/{0}/holdings/?apikey={1}"
    response = requests.get(url.format(mmsid, token))
    xml = etree.fromstring(response.content.strip())
    # TODO: Add missing collections' namespace:PID
    collections = {
        #'bizzell_bible': '//holdings/holding/library[text()="BIZZELL"] and //holdings/holding/location[text()="BIZZ_BIBLE"]',
        'oku:bbh': '//holdings/holding/library[text()="BIZZELL"] and //holdings/holding/location[text()="BASS_COLL"]',
        #'west_hist': '//holdings/holding/library[text()="WESTRNHIST"]',
        'oku:hos': '//holdings/holding/library[text()="HISTSCI"]',
    }
    for collection, path in collections.items():
        if xml.xpath(path):
            return collection


def list_missing_derivatives(count=0):
    """ list bags missing derivatives """
    db_client = app.backend.database.client
    digital_objects = db_client.catalog.digital_objects
    missing_derivatives = digital_objects.find({'derivatives':{}},{'bag':1})
    return [x['bag'] for x in list(missing_derivatives.limit(count))]


def list_missing_ingest():
    """ list bags with derivatives not yet ingested """
    pass


def generate_derivative(bagname):
    """ generate derivative and load into S3 """
    if s3_source_exists(bagname):
        mmsid = get_mmsid(bagname)
        if mmsid is not None:
            pass
            # download source items, generate derives, and export metadate
            # get alma record, convert to mods, get title
            # generate recipe, bag, upload to s3, remove working files
            # update digital catalog 
        else:
            print("Could not get MMSID for bag: {0}".format(bagname))
    else:
        print("Could not find source bag: {0}".format(bagname))


def process_derivatives():
    for bag in list_missing_derivatives():
        generate_derivative(bagname)


def ingest_derivative():
    for bag in list_missing_ingest():
        mmsid = get_mmsid(bag)
        collection = get_collection(mmsid) if mmsid else None
        if collection is not None:
            pass
            # call islandora remote worker to ingest bag
            # update digital catalog
        else:
            print("Could not determine collection for: {0}".format(bag))


