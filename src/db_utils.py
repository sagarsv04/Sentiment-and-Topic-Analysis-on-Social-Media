#!/usr/bin/env python3

import os
import sys
import configparser
import pandas as pd
from pymongo import MongoClient


base_dir = "../"
# base_dir = "./"

def get_db_credentials(logger):

    credentials = {}
    config = configparser.ConfigParser()
    config.read("{0}res/credentials".format(base_dir))
    credentials["ip"] = config["Database"]["ip"]
    credentials["port"] = config["Database"]["port"]
    credentials["name"] = config["Database"]["name"]
    credentials["twitter"] = config["Database"]["twitter"]
    credentials["reddit"] = config["Database"]["reddit"]
    credentials["twitter_stream"] = config["Database"]["twitter_stream"]
    logger.info("Done fetching DB credentials")
    return credentials


def connect_db(logger, credentials):

    db_client = None
    client = None
    try:
        client = MongoClient("mongodb://{0}:{1}/".format(credentials["ip"], credentials["port"]))
    except Exception as ex:
        logger.error("Error: {0}".format(ex))
        logger.info("Failed to connect to DB Client")

    if credentials["name"] in client.list_database_names():
        db_client = client[credentials["name"]]
    else:
        logger.info("Error: {0} Database not found on server".format(credentials["name"]))

    return db_client



def push_data_db(logger, db_client, collection_name, data):
    # data = twitter_data
    # collection_name = "weekly"

    ret_status = False
    is_many = False

    if len(data)>1:
        is_many = True

    try:
        db_collection = db_client[collection_name]
        if is_many:
            data_list = list(data.T.to_dict().values())
            rec = db_collection.insert_many(data_list)
            print("Success: Ids :: {0}".format(rec.inserted_ids))
        else:
            if len(data)>0:
                data_dict = list(data.T.to_dict().values())[0]
                rec = db_collection.insert_one(data_dict)
                print("Success: Id :: {0}".format(rec.inserted_id))
            else:
                logger.info("No data to insert to DB")
        ret_status = True
        logger.info("Stored {0} data into Database".format(collection_name))
    except Exception as ex:
        logger.error("Error: {0}".format(ex))
        logger.info("Failed to insert data to DB")
    return ret_status



def main():
    logger = data_log.get_logger("db_utils")
    credentials = get_db_credentials(logger)
    db_client = connect_db(logger, credentials)

    return 0



if __name__ == '__main__':
    main()
