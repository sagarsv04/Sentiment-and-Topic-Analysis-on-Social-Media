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
import numpy as np
import matplotlib.pyplot as plt
from dateutil.parser import parse
from datetime import datetime, date



def plot_data_size(size_bytes, data_name):

    days = np.arange(1,8)
    days_bytes = []

    for b_size in size_bytes:
        # b_size = size_bytes[0]
        days_bytes.append((days*b_size)/1000000)

    plt.figure(figsize=(12, 8))

    plt.plot(days, days_bytes[2], "o-", label=data_name[2])
    plt.plot(days, days_bytes[1], "o-", label=data_name[1])
    plt.plot(days, days_bytes[0], "o-", label=data_name[0])
    plt.grid(True)
    plt.yscale("log", basey=10)
    plt.title("Data Storage Size")
    plt.xlabel("Number of Days")
    plt.ylabel("Size in MB")
    plt.legend(loc="best")
    plt.tight_layout()
    # plt.show()
    plt.savefig("../out/size_graph.png")

    return 0



def main():
    size_bytes = [363778, 456319, 64410745]
    data_name = ["Twitter", "Reddit", "Twitter Stream"]

    plot_data_size(size_bytes, data_name)


    return 0


if __name__ == '__main__':
    main()
