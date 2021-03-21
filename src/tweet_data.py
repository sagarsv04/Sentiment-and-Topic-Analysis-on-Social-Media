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
from dateutil.parser import parse
from datetime import datetime, date
import data_log
import db_utils


base_dir = "../"
# base_dir = "./"

def get_twitter_credentials(logger):

    credentials = {}
    config = configparser.ConfigParser()
    config.read("{0}res/credentials".format(base_dir))
    credentials["api_key"] = config["Twitter"]["api_key"]
    credentials["api_secret_key"] = config["Twitter"]["api_secret_key"]
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

    key_secret = "{0}:{1}".format(credentials["api_key"], credentials["api_secret_key"]).encode("ascii")
    b64_encoded_key = base64.b64encode(key_secret)
    b64_encoded_key = b64_encoded_key.decode("ascii")

    auth_headers = {
        "Authorization": "Basic {0}".format(b64_encoded_key),
        "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8"
    }
    auth_data = {
        "grant_type": "client_credentials"
    }
    logger.info("Done creating auth headers")
    return auth_headers, auth_data


def get_twitter_data(auth_headers, auth_data, topics, logger):

    base_url = "https://api.twitter.com/"
    auth_url = "{0}oauth2/token".format(base_url)
    search_url = "{0}1.1/search/tweets.json".format(base_url)

    twitter_data = pd.DataFrame()
    todays_date = date.today().strftime("%m-%d-%Y")

    logger.info("Started Collecting Twitter Data")
    auth_resp = requests.post(auth_url, headers=auth_headers, data=auth_data)

    if auth_resp.status_code == 200:
        access_token = auth_resp.json()["access_token"]
        search_headers = {"Authorization": "Bearer {0}".format(access_token)}
        try:
            for pos in topics:
                # pos ="1"
                logger.info("Collecting Twitter Data for topic :{0}".format(topics[pos]))
                topic_data = pd.DataFrame()
                search_params = {"q": "{0}".format(topics[pos]), "result_type": "recent", "count": 1000}
                search_resp = requests.get(search_url, headers=search_headers, params=search_params)
                if search_resp.status_code == 200:
                    tweet_data = search_resp.json()
                    topic_data = topic_data.append(pd.DataFrame.from_dict(tweet_data["statuses"])[["text", "created_at", "id"]])
                    topic_data["topic"] = topics[pos]
                    topic_data["fetch_date"] = todays_date.split("_")[0]
                    topic_data["sentiment"] = ""
                else:
                    logger.error("Error: {0} : {1}".format(search_resp.status_code, search_resp.reason))
                twitter_data = twitter_data.append(topic_data)
            twitter_data = twitter_data.reset_index(drop=True)
            logger.info("Done Collecting Twitter Data")
        except Exception as ex:
            logger.error("Error: {0}".format(ex))
            logger.info("Twitter Data not collected")

        if len(twitter_data)>0:
            twitter_data = twitter_data.rename(columns={"created_at":"created_date", "id":"tweet_id"})
            twitter_data["created_date"] = twitter_data["created_date"].apply(lambda x: parse(x).strftime("%m-%d-%Y %H:%M:%S"))
            twitter_data["fetch_date"] = todays_date.split("_")[0]
            twitter_data["sentiment"] = ""
    else:
        logger.error("Error: {0} : {1}".format(auth_resp.status_code, auth_resp.reason))

    return twitter_data


def main():

    logger = data_log.get_logger("twitter_data")
    todays_date = datetime.now().strftime("%m-%d-%Y_%H_%M_%S")
    start_time = time.time()
    credentials = get_twitter_credentials(logger)
    topics = get_topics(logger)
    auth_headers, auth_data = format_api_headers(credentials, logger)
    twitter_data = get_twitter_data(auth_headers, auth_data, topics, logger)

    credentials = db_utils.get_db_credentials(logger)
    db_client = db_utils.connect_db(logger, credentials)

    if len(twitter_data)>0:
        if db_client != None:
            ret_status = db_utils.push_data_db(logger, db_client, "twitter", twitter_data)
        else:
            logger.info("Error: Twitter Data not uploaded to Database")

        with open("{0}out/twitter_data_{1}".format(base_dir, todays_date), "wb") as fp:
            pickle.dump(twitter_data, fp)
        logger.info("Stored Twitter Data at {0}".format("{0}out/twitter_data_{1}".format(base_dir, todays_date)))

        logger.info("Twitter Data Nos: {0} rows, Size: {1} bytes".format(len(twitter_data), sys.getsizeof(twitter_data)))
        logger.info("Twitter Execution Sec: {}".format((time.time()-start_time)))
    else:
        logger.info("Twitter Data Nos: {0} rows, Size: {1} bytes".format(len(twitter_data), sys.getsizeof(twitter_data)))
        logger.info("Twitter Data Nothing to store.")
    return 0



if __name__ == '__main__':
    main()
