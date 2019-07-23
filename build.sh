#!/bin/sh

#Note: to install the test version of the module, run the following:
# $ python3 -m pip install --index-url https://test.pypi.org/simple/ OntologyCrawler

#Check if 
if [ -d dist/ ] && [ "$(ls -A dist/)" ]; then
	rm -rf dist/*
fi
python setup.py sdist bdist_wheel
python -m twine upload --repository-url https://test.pypi.org/legacy/ dist/*