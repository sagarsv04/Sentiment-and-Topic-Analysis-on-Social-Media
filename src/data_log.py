#!/usr/bin/env python3
import os
import sys
import logging
from datetime import date



def get_logger(name):

	# create logger
	logger = logging.getLogger("{0}_logger".format(name))
	logger.setLevel(logging.INFO)

	# create console handler and set level to debug
	ch = logging.StreamHandler()
	ch.setLevel(logging.DEBUG)

	formatter = logging.Formatter('%(asctime)s - %(message)s')

	# add formatter to ch
	ch.setFormatter(formatter)

	# add ch to logger
	logger.addHandler(ch)

	if not os.path.isdir("../logs/"):
		os.mkdir("../logs/")


	fh = logging.FileHandler("../logs/{0}_{1}.log".format(name, date.today().strftime("%m_%d_%Y")))
	fh.setFormatter(formatter)
	logger.addHandler(fh)

	return logger



def main():

	logger = get_logger()

	return 0


if __name__ == '__main__':
	main()
