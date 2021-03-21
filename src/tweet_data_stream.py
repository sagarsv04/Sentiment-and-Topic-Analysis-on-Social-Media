#!/usr/bin/env python3

import os
import sys
import json
import time
import time
import base64
import requests
import pickle
import configparser
import pandas as pd
from datetime import datetime, date
import data_log
import db_utils


base_dir = "../"
# base_dir = "./"

stream_timeout = 3600


def get_twitter_credentials(logger):

    credentials = {}
    config = configparser.ConfigParser()
    config.read("{0}res/credentials".format(base_dir))
    credentials["api_key"] = config["Twitter"]["api_key"]
    credentials["api_secret_key"] = config["Twitter"]["api_secret_key"]
    credentials["stream_bearer_token"] = config["Twitter"]["stream_bearer_token"]
    logger.info("Done fetching credentials")
    return credentials


def get_topics(logger):

    topics = {}
    config = configparser.ConfigParser()
    config.read("{0}res/top_topics".format(base_dir))
    for pos in list(config["Topics"]):
        topics[pos] = config["Topics"][pos]
    logger.info("Done fetching topics")
    return topics


def format_api_headers(credentials, logger):

    auth_headers = {
        "Authorization": "Bearer {0}".format(credentials["stream_bearer_token"])
    }

    logger.info("Done creating auth headers")
    return auth_headers


def get_twitter_data(auth_headers, logger):

    base_url = "https://api.twitter.com/2/tweets/sample/stream"
    search_url = "{0}?tweet.fields=created_at".format(base_url)
    twitter_data = pd.DataFrame()
    todays_date = date.today().strftime("%m-%d-%Y")
    logger.info("Started Collecting Twitter Stream Data")
    try:
        start_time = time.time()
        search_resp = requests.request("GET", search_url, headers=auth_headers, stream=True)
        if search_resp.status_code == 200:
            for response_line in search_resp.iter_lines():
                current_time = time.time()
                if response_line:
                    json_response = json.loads(response_line)
                    twitter_data = twitter_data.append(pd.DataFrame.from_dict([json_response["data"]]))
                elif (current_time-start_time)>stream_timeout:
                    logger.info("Time Out")
        else:
            logger.error("Error: {0} : {1}".format(search_resp.status_code, search_resp.reason))
        twitter_data = twitter_data.reset_index(drop=True)

    except Exception as ex:
        logger.error("Error: {0}".format(ex))
    if len(twitter_data)>0:
        logger.info("Collected Twitter Stream Data")
        twitter_data = twitter_data.rename(columns={"created_at":"created_date", "id":"tweet_id"})
        twitter_data["created_date"] = twitter_data["created_date"].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%m-%d-%Y %H:%M:%S"))
        twitter_data["fetch_date"] = todays_date.split("_")[0]
        twitter_data["sentiment"] = ""

    return twitter_data


def main():

    logger = data_log.get_logger("twitter_stream")
    todays_date = datetime.now().strftime("%m-%d-%Y_%H_%M_%S")
    start_time = time.time()
    credentials = get_twitter_credentials(logger)
    topics = get_topics(logger)
    auth_headers = format_api_headers(credentials, logger)
    twitter_data = get_twitter_data(auth_headers, logger)
    credentials = db_utils.get_db_credentials(logger)
    db_client = db_utils.connect_db(logger, credentials)

    if len(twitter_data)>0:
        if db_client != None:
            ret_status = db_utils.push_data_db(logger, db_client, "twitter_stream", twitter_data)
        else:
            logger.info("Error: Stream Data not uploaded to Database")

        with open("{0}out/twitter_data_stream_{1}".format(base_dir, todays_date), "wb") as fp:
            pickle.dump(twitter_data, fp)
        logger.info("Stored Twitter Stream Data at {0}".format("{0}out/twitter_data_stream_{1}".format(base_dir, todays_date)))

        logger.info("Twitter Stream Data Nos: {0} rows, Size: {1} bytes".format(len(twitter_data), sys.getsizeof(twitter_data)))
        logger.info("Twitter Stream Execution Sec: {}".format((time.time()-start_time)))
    else:
        logger.info("Twitter Stream Data Nos: {0} rows, Size: {1} bytes".format(len(twitter_data), sys.getsizeof(twitter_data)))
        logger.info("Twitter Stream Data Nothing to store.")

    return 0



if __name__ == '__main__':
    main()
