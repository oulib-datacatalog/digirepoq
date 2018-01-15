from celery.task import task
from utils import search_mongodb, s3_source_exists, get_mmsid, get_collection
import logging


def list_missing_derivatives(count=0):
    """ list bags missing derivatives """
    return search_mongodb({'derivatives.jpeg_040_antialias':{"$exists":False}}, count)


def list_missing_ingest(count=0):
    """ list bags with derivatives not yet ingested """
    args = {"derivatives.jpeg_040_antialias":{"$exists":True},"application.islandora.ingested":{"$ne":True}}
    return search_mongodb(args, count)


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


