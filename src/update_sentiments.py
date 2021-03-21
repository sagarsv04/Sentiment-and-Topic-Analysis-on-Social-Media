#!/usr/bin/env python3

import os
import sys
import configparser
import pandas as pd
import data_log
import pickle
from datetime import datetime, date, timedelta
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

base_dir = "../"
# base_dir = "./"

# os.getcwd()
# os.chdir("{0}src".format(base_dir))

def get_data_credentials(logger):

    credentials = {}
    config = configparser.ConfigParser()
    config.read("{0}res/credentials".format(base_dir))
    credentials["ip"] = config["Database"]["ip"]
    credentials["port"] = config["Database"]["port"]
    credentials["name"] = config["Database"]["name"]
    credentials["twitter"] = config["Database"]["twitter"]
    credentials["reddit"] = config["Database"]["reddit"]
    credentials["twitter_stream"] = config["Database"]["twitter_stream"]
    logger.info("Done fetching Data credentials")
    return credentials


def get_run_dates(logger, start_date, end_date):
    # start_date = datetime(2020, 11, 19)
    # end_date = datetime(2020, 11, 19)
    # end_date = ""
    run_date_list = []

    try:
        assert (type(start_date) == datetime), "Expected Start Date to be DateTime objects"
        assert (type(end_date) == datetime), "Expected End Date to be DateTime objects"
        assert (start_date <= end_date), "Start Date Should be before|same compared to End Date"

        if start_date == end_date:
            run_date_list.append(start_date.strftime("%m-%d-%Y"))
        else:
            delta = end_date - start_date
            for i in range(delta.days + 1):
                run_date = start_date + timedelta(days=i)
                run_date_list.append(run_date.strftime("%m-%d-%Y"))
        logger.info("Done fetching run dates list")
    except Exception as ex:
        logger.error("Error: {0}".format(ex))
        logger.info("Failed fetching run dates list")
    return run_date_list


def get_date_files(run_date):

    file_list = []
    all_files = os.listdir("{0}out/".format(base_dir))

    for file in all_files:
        # file = all_files[-1]
        if len(file.split("."))==1:
            file_split = file.split("_")
            if file_split[2]!="stream":
                if file_split[2]==run_date:
                    file_list.append(file)
            else:
                if file_split[3]==run_date:
                    file_list.append(file)

    return file_list


def get_date_local_data(logger, file_name):

    file_data = None

    with open("{0}out/{1}".format(base_dir, file_name), "rb") as fp:
        file_data = pickle.load(fp)
    logger.info("Loaded Data from {0}".format("{0}out/{1}".format(base_dir, file_name)))

    return file_data


def store_date_local_data(logger, file_name, file_data):

    with open("{0}out/{1}".format(base_dir, file_name), "wb") as fp:
        pickle.dump(file_data, fp)
    logger.info("Updated Data for {0}".format("{0}out/{1}".format(base_dir, file_name)))

    return 0


def run_sentiment_update(logger, data, sid_obj):
    # data = file_data

    if "text" in data.columns:
        # sentiment_dict = sid_obj.polarity_scores(sentence)
        data["sentiment"] = data["text"].apply(lambda x : sid_obj.polarity_scores(x))
    else:
        logger.error("Error: Couldn't find 'text' column")
        logger.info("Data columns: {0}".format(list(data.columns)))

    return data


def run_update_for_dates(logger, credentials, run_dates):

    # Create a SentimentIntensityAnalyzer object.
    sid_obj = SentimentIntensityAnalyzer()

    for run_date in run_dates:
        # run_date = run_dates[0]
        logger.info("Running updates for date: {0}".format(run_date))
        file_list = get_date_files(run_date)

        # run on pickel
        if len(file_list)>0:
            for file_name in file_list:
                try:
                    # file_name = file_list[0]
                    file_data = get_date_local_data(logger, file_name)
                    file_data = run_sentiment_update(logger, file_data, sid_obj)
                    # file_data = data
                    store_date_local_data(logger, file_name, file_data)
                except Exception as ex:
                    logger.error("Error: {0}".format(ex))
                    logger.info("Falied to update local data for: {0}".format(file_name))
        else:
            logger.info("No pickle files for date: {0}".format(run_date))

        # run on db using same logic


    return 0


def main():

    logger = data_log.get_logger("update_sentiments")
    credentials = get_data_credentials(logger)
    run_dates = get_run_dates(logger, datetime(2020, 11, 20), datetime(2020, 12, 7))
    run_update_for_dates(logger, credentials, run_dates)

    return 0


if __name__ == '__main__':
    main()
