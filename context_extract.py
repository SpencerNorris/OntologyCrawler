#!/usr/bin/env python3
'''
File: context_extract.py
Author: Spencer Norris
Description: Read in a Java properties file
per the HADATAC spec and use it to discover
property paths for classes with the given
prefix.
'''


import argparse

from rdflib import Graph

#Read in Java property file
import javaproperties

#Home cookin' modules
from ontology_crawler import retrieve_crawl_paths


def expand(base_url, other_url):
	pass

def main(fin):
	#Read in properties file
	with open(fin, 'r') as fp:
		javaprops = javaproperties.load(fp)

	for k in javaprops.keys():
		print(javaprops[k].split(','))


if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument(
		'-f', '--file', type=str,
		nargs='?', default=None,
		help="Java properties file to read in. Must be set by user."
	)

	args = parser.parse_args()
	main(args.file)