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
from rdflib.namespace import RDFS, OWL
import javaproperties

#Home cookin' modules
from ontology_crawler import retrieve_crawl_paths_from_context



def extract_from_contexts(seed_iri,properties,property_f,extract_params,verbose=False,error=None):
	seed_graph = Graph().parse(seed_iri)
	print("Loaded seed graph.")

	SEED_QUERY_TEMPLATE = """
		PREFIX owl: <http://www.w3.org/2002/07/owl#>
		SELECT DISTINCT ?c WHERE{
			?c a owl:Class .
			FILTER(regex(str(?c), "%s"))
		}
		"""

	#Read in properties file
	with open(property_f, 'r') as fp:
		javaprops = javaproperties.load(fp)
	print("Read properties file.")

	for k in javaprops.keys():
		print("Reading graph: ", str(k))
		#Pull the ontology IRI and associated prefix
		row = javaprops[k].split(',')
		prefix = row[0]
		iri = row[2]
		#Check whether we have an IRI for the row
		if iri == '' or iri == seed_iri:
			continue
		else:
			#Read in graph from IRI, use as context
			context = Graph()
			FORMATS=['xml','n3','nt','trix','turtle','rdfa']
			read_success = False
			for form in FORMATS:
				try:
					#If we successfully read our ontology, recurse
					context = Graph().parse(str(row[0]),format=form)
					read_success = True
					print("Read as ", form, ".")
					break
				except Exception as e:
					pass
			if not read_success:
				if error is None:
					raise Exception("Exhausted format list. Failing quickly.")
				if error == 'ignore':
					print("Exhausted format list. Quietly ignoring failure.")

			#Expand property paths from context
			gout = retrieve_crawl_paths_from_context(	
				seed_graph, 
				context,
				properties,
				seed_query=SEED_QUERY_TEMPLATE % (prefix,),
				expand_ontologies=True,
				verbose=verbose,
				inplace=False,
				extract_params=extract_params)



if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument(
		'-f', '--file', type=str,
		nargs='?', default=None,
		help="Java properties file to read in. Must be set by user."
	)
	args = parser.parse_args()
	#Use CHEAR as our base graph
	SEED_IRI = 'http://hadatac.org/ont/chear/'
	PREDICATES = [ #predicates we'll recursively expand paths for 
		RDFS.subClassOf,
		OWL.equivalentClass
	]
	#Immediate subclasses, full superclass tree
	extract_params={
		'upstream' : True, 
		'downstream' : True, 
		'up_shallow' : True, 
		'down_shallow' : False
	}
	extract_from_contexts(
		seed_iri=SEED_IRI,
		properties=PREDICATES,
		property_f=args.file,
		verbose=True,
		extract_params=extract_params)