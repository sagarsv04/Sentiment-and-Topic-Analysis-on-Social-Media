#!/usr/bin/env python3

import os
import sys
import configparser
import pandas as pd
import numpy as np
import data_log
import pickle
import matplotlib.pyplot as plt
from datetime import datetime, date, timedelta


base_dir = "../"
# base_dir = "./"

# os.getcwd()
# os.chdir("{0}src".format(base_dir))


def get_topics(logger):

    topics = {}
    config = configparser.ConfigParser()
    config.read("{0}res/top_topics".format(base_dir))
    for pos in list(config["Topics"]):
        topics[pos] = config["Topics"][pos]
    logger.info("Done fetching topics")
    return topics


def get_date_local_data(logger, file_name, is_stream=False):

    file_data = None

    with open("{0}out/{1}".format(base_dir, file_name), "rb") as fp:
        file_data = pickle.load(fp)

    if is_stream:
        file_data["classify"] = file_data["sentiment"].apply(lambda x: "pos" if x["compound"]>=0.05 else ("neg" if x["compound"]<=-0.05 else "neu"))
        file_data = file_data[["classify"]]
    else:
        file_data["classify"] = file_data["sentiment"].apply(lambda x: "pos" if x["compound"]>=0.05 else ("neg" if x["compound"]<=-0.05 else "neu"))
        file_data = file_data[["topic", "classify"]]
    logger.info("Loaded Data from {0}".format("{0}out/{1}".format(base_dir, file_name)))

    return file_data


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


def plot_analysis(logger, analysis_data, analysis_name, is_stream=False):
    # analysis_data, analysis_name = twitter_analysis, "Twitter"
    # analysis_data, analysis_name = reddit_analysis, "Reddit"
    # analysis_data, analysis_name = twitter_stream_analysis, "Twitter Stream"

    date_list = analysis_data["date"].unique()
    topic_list = None
    if not is_stream:
        topic_list = analysis_data["topic"].unique()
        topic_list_analysis = []
        for topic in topic_list:
            # topic = topic_list[0]
            topic_pos = []
            topic_neu = []
            topic_neg = []
            topic_total = []
            for run_date in date_list:
                # run_date = date_list[0]
                topic_data = analysis_data.loc[(analysis_data["topic"]==topic)&(analysis_data["date"]==run_date)].reset_index(drop=True)
                topic_count = len(topic_data)
                pos_count = len(topic_data.loc[topic_data["classify"]=="pos"])
                neu_count = len(topic_data.loc[topic_data["classify"]=="neu"])
                neg_count = len(topic_data.loc[topic_data["classify"]=="neg"])
                topic_pos.append(pos_count)
                topic_neu.append(neu_count)
                topic_neg.append(neg_count)
                topic_total.append(topic_count)
            topic_list_analysis.append([topic_pos, topic_neu, topic_neg, topic_total])

            plt_marker = ''
            if analysis_name == "Twitter":
                plt_marker = 'D'
            elif analysis_name == "Reddit":
                plt_marker = 'o'

            plt.figure(figsize=(16,8))
            plt.style.use('seaborn')

            plt.plot(date_list, topic_pos, marker=plt_marker, color='green', linewidth=2, alpha=0.7, label="Positive")
            plt.plot(date_list, topic_neu, marker=plt_marker, color='blue', linewidth=2, alpha=0.7, label="Neutral")
            plt.plot(date_list, topic_neg, marker=plt_marker, color='red', linewidth=2, alpha=0.7, label="Negative")

            file_name = "{0} {1} Results {2}_{3}".format(analysis_name, topic, date_list[0], date_list[-1])
            plt.title(file_name)
            plt.xlabel("Date")
            plt.ylabel("Count")
            plt.legend(loc="best")
            plt.tight_layout()
            plt.savefig("{0}out/{1}.png".format(base_dir, file_name))
            # plt.show()
        topic_list_analysis = np.array(topic_list_analysis)
        return topic_list_analysis, topic_list, date_list
    else:
        date_pos = []
        date_neu = []
        date_neg = []
        for run_date in date_list:
            # run_date = date_list[0]
            date_data = analysis_data.loc[analysis_data["date"]==run_date].reset_index(drop=True)
            date_pos.append(len(date_data.loc[date_data["classify"]=="pos"]))
            date_neu.append(len(date_data.loc[date_data["classify"]=="neu"]))
            date_neg.append(len(date_data.loc[date_data["classify"]=="neg"]))

        plt.figure(figsize=(16,8))
        plt.style.use('seaborn')

        plt.plot(date_list, date_pos, marker='D', color='green', linewidth=2, alpha=0.7, label="Positive")
        plt.plot(date_list, date_neu, marker='D', color='blue', linewidth=2, alpha=0.7, label="Neutral")
        plt.plot(date_list, date_neg, marker='D', color='red', linewidth=2, alpha=0.7, label="Negative")
        file_name = "{0} Results {1}_{2}".format(analysis_name, date_list[0], date_list[-1])
        plt.title(file_name)
        plt.xlabel("Date")
        plt.ylabel("Count")
        plt.legend(loc="best")
        plt.tight_layout()
        plt.savefig("{0}out/{1}.png".format(base_dir, file_name))
        # plt.show()
        return 0


def plot_compare_analysis(logger, twitter_analysis_list, reddit_analysis_list, compare_name):
    # compare_name = "Twitter vs Reddit"

    if np.all(twitter_analysis_list[1]==reddit_analysis_list[1]):
        if np.all(twitter_analysis_list[2]==reddit_analysis_list[2]):
            date_list = twitter_analysis_list[2]
            for idx in range(len(twitter_analysis_list[1])):
                # idx = 0
                twitter_topic = twitter_analysis_list[0][idx]
                twitter_topic = twitter_topic/twitter_topic[3]*100
                reddit_topic = reddit_analysis_list[0][idx]
                reddit_topic = reddit_topic/reddit_topic[3]*100

                plt.figure(figsize=(18,10))
                plt.style.use('seaborn')

                plt.plot(date_list, reddit_topic[0], marker='o', color='green', linewidth=2, alpha=0.7, label="Reddit Positive")
                plt.plot(date_list, reddit_topic[1], marker='o', color='blue', linewidth=2, alpha=0.7, label="Reddit Neutral")
                plt.plot(date_list, reddit_topic[2], marker='o', color='red', linewidth=2, alpha=0.7, label="Reddit Negative")

                plt.plot(date_list, twitter_topic[0], marker='D', color='green', linewidth=2, alpha=0.7, label="Twitter Positive")
                plt.plot(date_list, twitter_topic[1], marker='D', color='blue', linewidth=2, alpha=0.7, label="Twitter Neutral")
                plt.plot(date_list, twitter_topic[2], marker='D', color='red', linewidth=2, alpha=0.7, label="Twitter Negative")

                file_name = "{0} {1} Results {2}_{3}".format(compare_name, twitter_analysis_list[1][idx], date_list[0], date_list[-1])
                plt.title(file_name)
                plt.xlabel("Date")
                plt.ylabel("Percentage")
                plt.legend(loc="best")
                plt.tight_layout()
                plt.savefig("{0}out/{1}.png".format(base_dir, file_name))
                # plt.show()
        else:
            logger.error("Error: Can't compare analysis of: {0}".format(compare_name))
            logger.info("Date list different {0} vs {1}".format(twitter_analysis_list[2], reddit_analysis_list[2]))
    else:
        logger.error("Error: Can't compare analysis of: {0}".format(compare_name))
        logger.info("Topic list different {0} vs {1}".format(twitter_analysis_list[1], reddit_analysis_list[1]))

    return 0


def plot_data_collection(logger, run_dates, collection_name):
    # collection_name = "Twitter Stream Data"

    run_date_time_list = []
    data_date_time_count = []
    for run_date in run_dates:
        # run_date = run_dates[0]
        file_list = get_date_files(run_date)
        # for twitter data
        if len(file_list)>0:
            for file_name in file_list:
                try:
                    # file_name = file_list[7]
                    file_split = file_name.split("_")
                    if file_split[0]=="twitter" and file_split[2]=="stream":
                        file_data = get_date_local_data(logger, file_name, True)
                        run_date_time_list.append("{0} {1}:{2}:{3}".format(file_split[3], file_split[4], file_split[5], file_split[6]))
                        data_date_time_count.append(len(file_data))
                except Exception as ex:
                    logger.error("Error: {0}".format(ex))
                    logger.info("Falied to collect local data for: {0}".format(file_name))
        else:
            logger.info("No pickle files for date: {0}".format(run_date))

    plt.figure(figsize=(24,10))
    plt.style.use('seaborn')

    plt.plot(run_date_time_list, data_date_time_count, marker='D', color='blue', linewidth=2, alpha=0.7, label="Twitter Stream")

    file_name = "{0} Collection Results {1}_{2}".format(collection_name, run_dates[0], run_dates[-1])
    plt.title(file_name)
    plt.xlabel("Date Time")
    plt.xticks(rotation=90)
    plt.ylabel("Tweets Count")
    plt.legend(loc="best")
    plt.tight_layout()
    plt.savefig("{0}out/{1}.png".format(base_dir, file_name))


    return 0


def run_analysis_for_dates(logger, topics, run_dates):

    twitter_analysis = pd.DataFrame()
    reddit_analysis = pd.DataFrame()
    twitter_stream_analysis = pd.DataFrame()

    for run_date in run_dates:
        # run_date = run_dates[0]
        file_list = get_date_files(run_date)
        # for twitter data
        if len(file_list)>0:
            for file_name in file_list:
                try:
                    # file_name = file_list[7]
                    file_split = file_name.split("_")
                    if file_split[0]=="twitter" and file_split[2]!="stream":
                        file_data = get_date_local_data(logger, file_name)
                        file_data["date"] = run_date
                        twitter_analysis = twitter_analysis.append(file_data)
                    elif file_split[0]=="reddit" and file_split[2]!="stream":
                        file_data = get_date_local_data(logger, file_name)
                        file_data["date"] = run_date
                        reddit_analysis = reddit_analysis.append(file_data)
                    elif file_split[0]=="twitter" and file_split[2]=="stream":
                        file_data = get_date_local_data(logger, file_name, True)
                        file_data["date"] = run_date
                        twitter_stream_analysis = twitter_stream_analysis.append(file_data)
                except Exception as ex:
                    logger.error("Error: {0}".format(ex))
                    logger.info("Falied to collect local data for: {0}".format(file_name))
        else:
            logger.info("No pickle files for date: {0}".format(run_date))

    table_dic = {}

    twitter_analysis = twitter_analysis.reset_index(drop=True)
    # twitter_analysis.describe()
    twitter_dict = {}

    twitter_dict["Positive"] = len(twitter_analysis.loc[twitter_analysis["classify"]=="pos"].reset_index(drop=True))/len(twitter_analysis)*100
    twitter_dict["Neutral"] = len(twitter_analysis.loc[twitter_analysis["classify"]=="neu"].reset_index(drop=True))/len(twitter_analysis)*100
    twitter_dict["Negative"] = len(twitter_analysis.loc[twitter_analysis["classify"]=="neg"].reset_index(drop=True))/len(twitter_analysis)*100

    table_dic["Twitter"] = twitter_dict

    reddit_analysis = reddit_analysis.reset_index(drop=True)
    # reddit_analysis.describe()

    reddit_dict = {}

    reddit_dict["Positive"] = len(reddit_analysis.loc[reddit_analysis["classify"]=="pos"].reset_index(drop=True))/len(reddit_analysis)*100
    reddit_dict["Neutral"] = len(reddit_analysis.loc[reddit_analysis["classify"]=="neu"].reset_index(drop=True))/len(reddit_analysis)*100
    reddit_dict["Negative"] = len(reddit_analysis.loc[reddit_analysis["classify"]=="neg"].reset_index(drop=True))/len(reddit_analysis)*100

    table_dic["Reddit"] = reddit_dict

    twitter_stream_analysis = twitter_stream_analysis.reset_index(drop=True)
    # twitter_stream_analysis.describe()

    stream_dict = {}

    stream_dict["Positive"] = len(twitter_stream_analysis.loc[twitter_stream_analysis["classify"]=="pos"].reset_index(drop=True))/len(twitter_stream_analysis)*100
    stream_dict["Neutral"] = len(twitter_stream_analysis.loc[twitter_stream_analysis["classify"]=="neu"].reset_index(drop=True))/len(twitter_stream_analysis)*100
    stream_dict["Negative"] = len(twitter_stream_analysis.loc[twitter_stream_analysis["classify"]=="neg"].reset_index(drop=True))/len(twitter_stream_analysis)*100

    table_dic["Twitter Stream"] = stream_dict

    table_data = pd.DataFrame.from_dict(table_dic)

    twitter_analysis_list = plot_analysis(logger, twitter_analysis, "Twitter")
    reddit_analysis_list = plot_analysis(logger, reddit_analysis, "Reddit")
    plot_analysis(logger, twitter_stream_analysis, "Twitter Stream", True)

    plot_compare_analysis(logger, twitter_analysis_list, reddit_analysis_list, "Twitter vs Reddit")
    return 0


def get_topics_analysis_for_dates(logger, topic, run_dates):
    # topic = "covid"

    twitter_analysis_df = pd.DataFrame()
    reddit_analysis_df = pd.DataFrame()

    for run_date in run_dates:
        # run_date = run_dates[0]
        file_list = get_date_files(run_date)
        # for twitter data
        if len(file_list)>0:
            for file_name in file_list:
                try:
                    # file_name = file_list[0]
                    file_split = file_name.split("_")
                    if file_split[0]=="twitter" and file_split[2]!="stream":
                        file_data = get_date_local_data(logger, file_name)
                        file_data["date"] = run_date
                        file_data = file_data.loc[file_data["topic"]==topic]
                        twitter_analysis_df = twitter_analysis_df.append(file_data)
                    elif file_split[0]=="reddit" and file_split[2]!="stream":
                        file_data = get_date_local_data(logger, file_name)
                        file_data["date"] = run_date
                        file_data = file_data.loc[file_data["topic"]==topic]
                        reddit_analysis_df = reddit_analysis_df.append(file_data)
                    elif file_split[0]=="twitter" and file_split[2]=="stream":
                        pass
                except Exception as ex:
                    logger.error("Error: {0}".format(ex))
                    logger.info("Falied to collect local data for: {0}".format(file_name))
        else:
            logger.info("No pickle files for date: {0}".format(run_date))

    twitter_analysis_df = twitter_analysis_df.reset_index(drop=True)
    # twitter_analysis_df.describe()

    reddit_analysis_df = reddit_analysis_df.reset_index(drop=True)
    # reddit_analysis_df.describe()

    twitter_topic_pos = []
    twitter_topic_neu = []
    twitter_topic_neg = []
    twitter_topic_total = []

    reddit_topic_pos = []
    reddit_topic_neu = []
    reddit_topic_neg = []
    reddit_topic_total = []

    for run_date in run_dates:
        # run_date = run_dates[0]
        twitter_topic_data = twitter_analysis_df.loc[(twitter_analysis_df["topic"]==topic)&(twitter_analysis_df["date"]==run_date)].reset_index(drop=True)
        twitter_topic_pos.append(len(twitter_topic_data.loc[twitter_topic_data["classify"]=="pos"]))
        twitter_topic_neu.append(len(twitter_topic_data.loc[twitter_topic_data["classify"]=="neu"]))
        twitter_topic_neg.append(len(twitter_topic_data.loc[twitter_topic_data["classify"]=="neg"]))
        twitter_topic_total.append(len(twitter_topic_data))

        reddit_topic_data = reddit_analysis_df.loc[(reddit_analysis_df["topic"]==topic)&(reddit_analysis_df["date"]==run_date)].reset_index(drop=True)
        reddit_topic_pos.append(len(reddit_topic_data.loc[reddit_topic_data["classify"]=="pos"]))
        reddit_topic_neu.append(len(reddit_topic_data.loc[reddit_topic_data["classify"]=="neu"]))
        reddit_topic_neg.append(len(reddit_topic_data.loc[reddit_topic_data["classify"]=="neg"]))
        reddit_topic_total.append(len(reddit_topic_data))

    twitter_analysis = np.array([twitter_topic_pos, twitter_topic_neu, twitter_topic_neg, twitter_topic_total])
    reddit_analysis = np.array([reddit_topic_pos, reddit_topic_neu, reddit_topic_neg, reddit_topic_total])

    return twitter_analysis, reddit_analysis


def get_stream_analysis_for_dates(logger, run_dates):
    # topic = "covid"

    twitter_stream_df = pd.DataFrame()
    twitter_pos = []
    twitter_neu = []
    twitter_neg = []
    twitter_total = []

    for run_date in run_dates:
        # run_date = run_dates[0]
        file_list = get_date_files(run_date)
        # for twitter data
        if len(file_list)>0:
            for file_name in file_list:
                try:
                    # file_name = file_list[0]
                    file_split = file_name.split("_")
                    if file_split[0]=="twitter" and file_split[2]!="stream":
                        pass
                    elif file_split[0]=="reddit" and file_split[2]!="stream":
                        pass
                    elif file_split[0]=="twitter" and file_split[2]=="stream":
                        file_data = get_date_local_data(logger, file_name, True)
                        file_data["date"] = run_date
                        twitter_pos.append(len(file_data.loc[file_data["classify"]=="pos"]))
                        twitter_neu.append(len(file_data.loc[file_data["classify"]=="neu"]))
                        twitter_neg.append(len(file_data.loc[file_data["classify"]=="neg"]))
                        twitter_total.append(len(file_data))
                except Exception as ex:
                    logger.error("Error: {0}".format(ex))
                    logger.info("Falied to collect local data for: {0}".format(file_name))
        else:
            logger.info("No pickle files for date: {0}".format(run_date))

    twitter_stream_analysis = np.array([twitter_pos, twitter_neu, twitter_neg, twitter_total])

    return twitter_stream_analysis



def main():

    logger = data_log.get_logger("sentiment_analysis")
    topics = get_topics(logger)
    run_dates = get_run_dates(logger, datetime(2020, 11, 21), datetime(2020, 12, 5))
    run_analysis_for_dates(logger, topics, run_dates)
    plot_data_collection(logger, run_dates, "Twitter Stream Data")

    return 0



if __name__ == '__main__':
    main()
