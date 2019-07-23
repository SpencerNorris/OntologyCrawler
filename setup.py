#!/usr/bin/env python
'''
File: setup.py
Author: Spener Norris
Description: It's a setup.py!
'''
import setuptools
from distutils.core import setup

with open("README.md", "r") as fh:
    long_description = fh.read()
setup(
	name='OntologyCrawler',
	author='Spencer Norris',
	author_email='spencernorris.datascience@gmail.com',
	url='https://github.com/tetherless-world/OntologyCrawler',
	description="Package for crawling property paths across multiple OWL ontologies in different execution environments.",
	long_description=long_description,
	version='0.1.3',
	license='Apache 2.0',
	packages=['OntologyCrawler'],
	scripts=['bin/context_extract.py'],
	setup_requires=['wheel'],
	classifiers=[
		"Programming Language :: Python :: 3.7",
		"License :: OSI Approved :: Apache Software License"
	]
)
