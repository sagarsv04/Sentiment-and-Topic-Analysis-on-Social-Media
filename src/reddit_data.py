#!/usr/bin/env python3

import os
import sys
import json
import time
import base64
import urllib
import pickle
import requests
from datetime import datetime, date
import configparser
import pandas as pd
import data_log
import db_utils


base_dir = "../"
# base_dir = "./"

def get_reddit_credentials(logger):

    credentials = {}
    config = configparser.ConfigParser()
    config.read("{0}res/credentials".format(base_dir))
    credentials["username"] = config["Reddit"]["username"]
    credentials["password"] = config["Reddit"]["password"]
    credentials["app_name"] = config["Reddit"]["app_name"]
    credentials["public_key"] = config["Reddit"]["public_key"]
    credentials["secret_key"] = config["Reddit"]["secret_key"]
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


def get_reddit_data(credentials, topics, logger):

    base_url = "https://www.reddit.com/"
    search_url = "{0}search.json?q={1}&t=day&limit=100"
    headers = {"User-Agent": "osx:r/relationships.single.result:v1.0 (by /u/{0})".format(credentials["username"])}
    todays_date = date.today().strftime("%m-%d-%Y")
    reddit_data = pd.DataFrame()
    logger.info("Started Collecting Reddit Data")
    try:
        for pos in topics:
            # pos ="1"
            logger.info("Collecting Reddit Data for topic :{0}".format(topics[pos]))
            req_url = search_url.format(base_url, topics[pos]).replace(" ", "%20")
            search_req = urllib.request.Request(req_url, headers=headers)
            text_data = urllib.request.urlopen(search_req).read()
            search_data = json.loads(text_data)
            search_data = pd.DataFrame.from_dict(search_data)
            children_data = pd.DataFrame.from_dict(search_data.loc["children", "data"])

            for idx in range(100):
                # idx = 0
                last = children_data.loc[len(children_data)-1, "data"]["name"]
                re_req_url = req_url+"&after={0}".format(last)
                try:
                    search_req = urllib.request.Request(re_req_url, headers=headers)
                    text_data = urllib.request.urlopen(search_req).read()
                    search_data = json.loads(text_data)
                    search_data = pd.DataFrame.from_dict(search_data)
                    children_data = children_data.append(pd.DataFrame.from_dict(search_data.loc["children", "data"])).reset_index(drop=True)
                    time.sleep(1)
                except Exception as ex:
                    logger.error("Error: {0}".format(ex))
                    logger.info("Stopped fetching after {0} for topic {1}".format(last, topics[pos]))
                    break;

            topic_data = pd.DataFrame()
            for idx in range(len(children_data)):
                # idx = 0
                post_df = pd.DataFrame.from_dict([children_data.loc[idx, "data"]])[["title", "created_utc", "num_comments"]]
                post_df = post_df.rename(columns={"title":"text", "created_utc":"created_date"})
                post_df["created_date"] = time.strftime("%m-%d-%Y %H:%M:%S", time.gmtime(post_df["created_date"][0]))
                topic_data = topic_data.append(post_df)
            topic_data = topic_data.reset_index(drop=True)
            topic_data["topic"] = topics[pos]
            reddit_data = reddit_data.append(topic_data)
        reddit_data["fetch_date"] = todays_date.split("_")[0]
        reddit_data["sentiment"] = ""
        reddit_data = reddit_data.reset_index(drop=True)
        logger.info("Done Collecting Reddit Data")
    except Exception as ex:
        logger.error("Error: {0}".format(ex))
        logger.info("Reddit Data not collected")

    return reddit_data



def main():

    logger = data_log.get_logger("reddit_data")
    todays_date = datetime.now().strftime("%m-%d-%Y_%H_%M_%S")
    start_time = time.time()
    credentials = get_reddit_credentials(logger)
    topics = get_topics(logger)
    reddit_data = get_reddit_data(credentials, topics, logger)

    credentials = db_utils.get_db_credentials(logger)
    db_client = db_utils.connect_db(logger, credentials)

    if len(reddit_data)>0:
        if db_client != None:
            ret_status = db_utils.push_data_db(logger, db_client, "reddit", reddit_data)
        else:
            logger.info("Error: Reddit Data not uploaded to Database")

        with open("{0}out/reddit_data_{1}".format(base_dir, todays_date), "wb") as fp:
            pickle.dump(reddit_data, fp)
        logger.info("Stored Reddit Data at {0}".format("{0}out/reddit_data_{1}".format(base_dir, todays_date)))

        logger.info("Reddit Data Nos: {0} rows, Size: {1} bytes".format(len(reddit_data), sys.getsizeof(reddit_data)))
        logger.info("Reddit Execution Sec: {}".format((time.time()-start_time)))
    else:
        logger.info("Reddit Data Nos: {0} rows, Size: {1} bytes".format(len(reddit_data), sys.getsizeof(reddit_data)))
        logger.info("Reddit Data Nothing to store.")
    return 0



if __name__ == '__main__':
    main()
