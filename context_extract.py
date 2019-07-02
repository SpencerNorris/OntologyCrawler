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

#Read in Java property file
from jproperties import Properties

#Home cookin' modules
from 

def main(fin):
	#Read in properties file
	javaprops = Properties()
	with open(fin,'r') as f:
		javaprops.load(f, "utf-8")
	print(javaprops)


if __name__ == '__main__':
	parser = argparse.ArgumentParser()

	parser.add_argument(
		'-f', '--file', type=str,
		nargs='?', default=None,
		help="Java properties file to read in. Must be set by user."
	) 
	main()