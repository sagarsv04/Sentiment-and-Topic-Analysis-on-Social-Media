#!/usr/bin/env python3
import os
import sys
import pickle
import pandas as pd
from datetime import datetime, date, timedelta

import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objects as go
from dash.dependencies import Input, Output


import data_log
import sentiment_analysis as s_analys

base_dir = "../"
# base_dir = "./"

# os.getcwd()
# os.chdir("{0}src".format(base_dir))


def format_topic_option(topics):

    topics_list = []
    for key in topics:
        topics_list.append({'label': topics[key].title(), 'value': topics[key]})
    return topics_list

def format_run_date_option():

    start = datetime(2020, 11, 21)
    end = datetime(2020, 12, 6)
    date_generated = [start + timedelta(days=x) for x in range(0, (end-start).days)]
    date_list = []
    for date in date_generated:
        # date = date_generated[0]
        date_list.append({'label': date.strftime('%b %d %Y'), 'value': date.strftime('%m-%d-%Y')})
    return date_list


def format_data_types_option():

    data_type_list = []
    data_type_list.append({'label': "Twitter Data", 'value': "twitter_data"})
    data_type_list.append({'label': "Reddit Data", 'value': "reddit_data"})
    data_type_list.append({'label': "Twitter vs Reddit", 'value': "twitter_reddit"})
    data_type_list.append({'label': "Twitter Data Stream", 'value': "twitter_data_stream"})

    return data_type_list


def get_app_layout(topics, date_list):

    layout = html.Div(
        children=[
            html.Div(className='row',
                children=[
                    html.Div(className='three columns div-user-controls',
                        children=[
                            html.H2('Sentiment and Topic Analysis on Social Media'),
                            html.Br(),
                            html.Br(),
                            html.Div(className='two row date-user-controls',
                                children=[
                                    html.P('Start Date.'),
                                    html.Div(className='div-for-dropdown',
                                        children=[
                                            dcc.Dropdown(id='startdateselector', options=date_list,
                                                style={'backgroundColor': '#1E1E1E'},
                                                className='startdateselector')],
                                        style={'color': '#1E1E1E'}),
                                    html.Details('Start Date should be less than End Date.'),
                                    html.Br(),
                                    html.P('End Date.'),
                                    html.Div(className='div-for-dropdown',
                                        children=[
                                            dcc.Dropdown(id='enddateselector', options=date_list,
                                                style={'backgroundColor': '#1E1E1E'},
                                                className='startdateselector')],
                                        style={'color': '#1E1E1E'}),
                                    html.Details('End Date should be greter than Start Date.')
                                    ]),
                            html.Br(),
                            html.P('Select data type from the dropdown below.'),
                            html.Div(className='div-for-dropdown',
                                children=[
                                    dcc.Dropdown(id='dataselector', options=format_data_types_option(),
                                        style={'backgroundColor': '#1E1E1E'},
                                        className='dataselector')],
                                style={'color': '#1E1E1E'}),
                            html.Br(),
                            html.P('Select a topic from the dropdown below.'),
                            html.Div(className='div-for-dropdown',
                                children=[
                                    dcc.Dropdown(id='topicselector', options=format_topic_option(topics),
                                        style={'backgroundColor': '#1E1E1E'},
                                        className='topicselector')],
                                style={'color': '#1E1E1E'}),
                            html.Details('Topic option is not avaliable for Twitter Data Stream Data.')
                        ]),
                    html.Div(className='nine columns div-for-charts bg-grey',
                        children=[
                            html.Br(),
                            html.H2('Graph Analysis',
                                style={'textAlign': 'center'}),
                            html.Br(),
                            # html.Div(className='eleven columns', id='analysisgraph',
                            html.Div(className='eleven columns',
                                children=[dcc.Graph(id='analysisgraph',
                                style={'height': '70vh'})],
                                style={'display': 'centre'}),
                            html.Br(),
                            html.P('Summary...'),
                            html.Br(),
                            html.P('# Topic wise Analysis: In this analysis, given a topic and a date range, sentiment trend for a platform is displayed'),
                            html.P('-- For example: if start date: 11/21/2020, end date: 12/7/2020, topic: Covid and Platform: Twitter, graph showing how all the sentiments are changing over a given time is displayed.'),
                            html.Br(),
                            html.P('# Comparative Analysis: In comparative analysis, for a given topic and a date range, sentiment trend of both platform is displayed'),
                            html.P('-- For example: if start date: 11/21/2020, end date: 12/7/2020, topic: Covid and Platform: Twitter vs Reddit, graph showing the sentiments comparative to both the platform changing over a given time is displayed.'),
                            html.Br(),
                            html.P('# Twitter Stream Analysis: In Stream analysis, the topic and not required and for a given date range, sentiment trend for a platform is displayed'),
                            html.P('-- For example: if start date: 11/21/2020, end date: 12/7/2020 and Platform: Twitter Data Stream, graph showing the sentiments changing over a given time is displayed.'),
                        ])
                ])
        ])

    return layout


def run_dashboard(logger, topics):

    date_list = format_run_date_option()

    app = dash.Dash(__name__)

    app.layout = get_app_layout(topics, date_list)

    @app.callback(
        Output(component_id='analysisgraph', component_property='figure'),
        [Input(component_id='startdateselector', component_property='value'),
        Input(component_id='enddateselector', component_property='value'),
        Input(component_id='dataselector', component_property='value'),
        Input(component_id='topicselector', component_property='value')]
    )
    def update_graphs(start_date, end_date, data_type, topic):

        ret = {}
        # start_date, end_date = "11-22-2020", "11-23-2020"
        if start_date!=None and end_date!=None and data_type!=None:
            start_date = datetime.strptime(start_date, "%m-%d-%Y")
            end_date = datetime.strptime(end_date, "%m-%d-%Y")
            if start_date < end_date:
                run_dates = s_analys.get_run_dates(logger, start_date, end_date)
                if data_type != "twitter_data_stream":
                    if topic!=None:
                        twitter_analysis, reddit_analysis = s_analys.get_topics_analysis_for_dates(logger, topic, run_dates)
                        if data_type == "twitter_data":
                            ret = {
                                    'data': [go.Scatter(x=run_dates, y=twitter_analysis[0], mode='lines+markers', name='Positive Results'),
                                            go.Scatter(x=run_dates, y=twitter_analysis[1], mode='lines+markers', name='Neutral Results'),
                                            go.Scatter(x=run_dates, y=twitter_analysis[2], mode='lines+markers', name='Negative Results')
                                        ],
                                    'layout': {
                                        'title': "'{0}' Sentiment Results on Twitter".format(topic),
                                        'xaxis': {'title': 'Dates'},
                                        'yaxis': {'title': 'Count'}
                                    }
                                }
                        elif data_type == "reddit_data":
                            ret = {
                                    'data': [go.Scatter(x=run_dates, y=reddit_analysis[0], mode='lines+markers', name='Positive Results'),
                                            go.Scatter(x=run_dates, y=reddit_analysis[1], mode='lines+markers', name='Neutral Results'),
                                            go.Scatter(x=run_dates, y=reddit_analysis[2], mode='lines+markers', name='Negative Results')
                                        ],
                                    'layout': {
                                        'title': "'{0}' Sentiment Results on Reddit".format(topic),
                                        'xaxis': {'title': 'Dates'},
                                        'yaxis': {'title': 'Count'}
                                    }
                                }
                        elif data_type == "twitter_reddit":
                            twitter_analysis = twitter_analysis/twitter_analysis[3]*100
                            reddit_analysis = reddit_analysis/reddit_analysis[3]*100
                            ret = {
                                    'data': [go.Scatter(x=run_dates, y=twitter_analysis[0], mode='lines+markers', name='Twitter Positive'),
                                            go.Scatter(x=run_dates, y=twitter_analysis[1], mode='lines+markers', name='Twitter Neutral'),
                                            go.Scatter(x=run_dates, y=twitter_analysis[2], mode='lines+markers', name='Twitter Negative'),
                                            go.Scatter(x=run_dates, y=reddit_analysis[0], mode='lines+markers', name='Reddit Positive'),
                                            go.Scatter(x=run_dates, y=reddit_analysis[1], mode='lines+markers', name='Reddit Neutral'),
                                            go.Scatter(x=run_dates, y=reddit_analysis[2], mode='lines+markers', name='Reddit Negative')
                                        ],
                                    'layout': {
                                        'title': "'{0}' Comparitive Sentiment Results".format(topic),
                                        'xaxis': {'title': 'Dates'},
                                        'yaxis': {'title': 'Percentage'}
                                    }
                                }
                        else:
                            logger.info("Required Valid Data Type: {0}".format(data_type))
                    else:
                        logger.info("Required Valid Topic: {0}".format(topic))
                elif data_type == "twitter_data_stream":
                    twitter_stream_analysis = s_analys.get_stream_analysis_for_dates(logger, run_dates)
                    ret = {
                            'data': [go.Scatter(x=run_dates, y=twitter_stream_analysis[0], mode='lines+markers', name='Positive Results'),
                                    go.Scatter(x=run_dates, y=twitter_stream_analysis[1], mode='lines+markers', name='Neutral Results'),
                                    go.Scatter(x=run_dates, y=twitter_stream_analysis[2], mode='lines+markers', name='Negative Results')
                                ],
                            'layout': {
                                'title': "Sentiment Results on Twitter Stream",
                                'xaxis': {'title': 'Dates'},
                                'yaxis': {'title': 'Count'}
                            }
                        }
                else:
                    logger.info("Required Valid Data Type: {0}".format(data_type))
            else:
                logger.warn("InValid Start and End Date.")
                logger.info("Start Date: {0}, End Date: {1}".format(start_date, end_date))
        else:
            logger.info("Not Enough Inputs: Start Date: {0}, End Date: {1}, Data Type: {2}".format(start_date, end_date, data_type))

        return ret

    app.run_server(debug=True)
    return 0



def main():
    logger = data_log.get_logger("analysis_dashboard")
    topics = s_analys.get_topics(logger)
    run_dashboard(logger, topics)

    return 0



if __name__ == '__main__':
    main()
