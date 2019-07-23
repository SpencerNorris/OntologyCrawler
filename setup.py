#!/usr/bin/env python
'''
File: setup.py
Author: Spener Norris
Description: It's a setup.py!
'''
from distutils.core import setup

setup(
	name='OntologyCrawler',
	author='Spencer Norris',
	author_email='spencernorris.datascience@gmail.com',
	url='https://github.com/tetherless-world/OntologyCrawler',
	version='0.1',
	packages=['OntologyCrawler'],
	scripts=['bin/context_extract']
)
