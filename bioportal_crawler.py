#!/usr/bin/env python
# coding: utf-8

'''
File: bioportal_crawler.py
Author: Spencer Norris
Description:

'''
import os
import json
import traceback
from rdflib import Graph, URIRef
from rdflib.namespace import RDF, RDFS, OWL
from SPARQLWrapper import SPARQLWrapper, JSON


# def report_bioportal():
# 	global seen
# 	global bioportal_graph
# 	#TODO: Expand with more information on 
# 	print("Number of BioPortal superclasses: ", len(seen))


def extract_bioportal_property_paths(
	seeds,bioportal,properties,
	downstream=True,upstream=True,
	up_shallow=False,down_shallow=False,
	verbose=False):
	'''
	This is a recursive method that will move all
	the way up the inheritance tree until we hit 
	superclass bedrock for the class we've been given.
	The recursion should follow the path of a directed
	acyclic graph, with one sink node, owl:Thing.


	params:
	k --> our class that we want to expand
	klasses --> set of classes already expanded.
	'''
	bioportal_graph = Graph()


	def _query_bioportal_downstream(k):
		'''
		Retrieve the next level downstream of the predicate path
		using the class k. Return a set of the leaf nodes.
		'''
		nonlocal bioportal
		nonlocal properties

		#Construct filter and query BioPortal for paths
		filter_str = "FILTER(" + " || ".join(["?pred = <%s>" % (pred,) for pred in properties]) + ")"
		query = """
		SELECT DISTINCT ?pred ?kn WHERE {
			<%s> ?pred ?kn.
			%s
		}
		""" % (str(k),filter_str)
		bioportal.setQuery(query)
		bioportal.setReturnFormat(JSON)
		results = bioportal.query().convert()

		#Add results to graph
		for result in results['results']['bindings']:
			pred = URIRef(result['pred']['value'])
			k_n = URIRef(result['kn']['value'])
			bioportal_graph.add( (k, pred, k_n) )
		
		#Return set of all downstream leaf nodes
		return {URIRef(result['kn']['value']) for result in results['results']['bindings']}


	def _query_bioportal_upstream(k):
		'''
		Retrieve the next level upstream of the predicate path
		using the class k. Return a set of the leaf nodes.
		'''
		nonlocal bioportal
		nonlocal properties

		#Construct filter and query BioPortal for paths
		filter_str = "FILTER(" + " || ".join(["?pred = <%s>" % (pred,) for pred in properties]) + ")"
		query = """
		SELECT DISTINCT ?pred ?kn WHERE {
			?kn ?pred <%s> .
			%s
		}
		""" % (str(k),filter_str)
		bioportal.setQuery(query)
		bioportal.setReturnFormat(JSON)
		results = bioportal.query().convert()

		#Add results to graph
		for result in results['results']['bindings']:
			pred = URIRef(result['pred']['value'])
			k_n = URIRef(result['kn']['value'])
			bioportal_graph.add( (k_n, pred, k) )
		
		#Return set of all downstream leaf nodes
		return {URIRef(result['kn']['value']) for result in results['results']['bindings']}


	seen_downstream = set()
	def _crawl_bioportal_downstream(k,i):
		'''
		Handler method for recursive downstream search of BioPortal.
		All recursion logic handled here; separate query handler
		reaches out with entities to BioPortal.
		'''
		nonlocal seen_downstream
		nonlocal down_shallow
		nonlocal verbose

		#For sanity checks on BioPortal recursion
		if verbose:
			print("Recursion level: ", i)
			print("Node: ", str(k))
		#If we've already expanded this node, don't recurse
		if k in seen_downstream:
			if verbose:
				print("Already seen ", k, ", skipping.")
			return
		else:				
			#Note that we're about to expand the parent
			if verbose:
				print("Not seen, expanding...")
			seen_downstream.add(k)

		#Retrieve all parents, properties for connecting back to input class
		parents = _query_bioportal_downstream(k)
		#Check whether to do depth=1
		if not down_shallow:
			#Go over all of the classes that were retrieved, if any
			for k_n in parents:
				#Expand our next node
				_crawl_bioportal_downstream(k_n,i+1)
		else:
			return

	seen_upstream = set()
	def _crawl_bioportal_upstream(k,i):
		'''
		Handler method for recursive upstream search of BioPortal.
		All recursion logic handled here; separate query handler
		reaches out with entities to BioPortal.
		'''
		nonlocal seen_upstream
		nonlocal up_shallow
		nonlocal verbose

		#For sanity checks on BioPortal recursion
		if verbose:
			print("Recursion level: ", i)
			print("Node: ", str(k))
		#If we've already expanded this node, don't recurse
		if k in seen_upstream:
			if verbose:
				print("Already seen!")
			return
		else:				
			#Note that we're about to expand thedown_shallow parent
			if verbose:
				print("Not seen, expanding...")
			seen_upstream.add(k)

		#Retrieve all parents, properties for connecting back to input class
		parents = _query_bioportal_upstream(k)
		#Check whether to do depth=1
		if not up_shallow:
			#Go over all of the classes that were retrieved, if any
			for k_n in parents:
				#Expand our next node
				_crawl_bioportal_upstream(k_n,i+1)
		else:
			return


	#Main section of extract_bioportal_property_paths
	for k in seeds:
		if downstream:
			if verbose:
				print(k + ": crawling downstream.")
			_crawl_bioportal_downstream(k,0)
		if upstream:
			if verbose:
				print(k + ": crawling upstream.")
			_crawl_bioportal_upstream(k,0)

	return bioportal_graph



def bioportal_retrieve_crawl_paths(
	properties,
	bioportal,
	seeds=None,seed_query=None,
	verbose=False,
	extract_params={'upstream' : True, 'downstream' : True, 'up_shallow' : False, 'down_shallow' : False}):
	'''
	Executes the seed query against BioPortal if necessary,
	then passes the results into extract_bioportal_property_paths.
	Basically just a convenience method that will imitate 
	'''
	#Helper method
	def _bioportal_retrieve_seed_classes(bioportal,seed_query):
		#Perform setup for SPARQLWrapper query, execute
		bioportal.setQuery(seed_query)
		bioportal.setReturnFormat(JSON)
		results = bioportal.query().convert()
		#There should only be one variable retreived,
		#  so this should work
		key = results['head']['vars'][0]
		#Return results
		return {result[key]['value'] for result in results['results']['bindings']}


	#We want either a list of seeds or a seed query, not both
	if (seed_query is None and seeds is None) or (seed_query is not None and seeds is not None):
		raise Exception("seed_query and seeds are mutually exclusive parameters. Please set exactly one.")

	#Collect the initial seed of classes to expand
	# This will look on BioPortal for the classes;
	# for local extracts you will want to pass a 'seeds' parameter.
	if seed_query is not None and seeds is None:
		seeds = _bioportal_retrieve_seed_classes(bioportal,seed_query)
	if verbose:
		if len(seeds) > 0:
			print("Number of seed classes: ", len(seeds))
			print("Sample classes: ")
			sample_size = 10 if len(seeds) >= 10 else len(seeds)
			for i in range(sample_size):
				print(list(seeds)[i])
		else:
			print("seed_query didn't retrieve any classes.")

	entity_graph = Graph()
	return entity_graph + extract_bioportal_property_paths(
							seeds=seeds,
							bioportal=bioportal,
							properties=properties,
							verbose=verbose,
							**extract_params)



if __name__ == '__main__':
	#Adapted from https://github.com/ncbo/sparql-code-examples/blob/master/python/sparql1.py
	BIOPORTAL_API_KEY = os.environ['BIOPORTAL_API_KEY']
	bioportal = SPARQLWrapper('http://sparql.bioontology.org/sparql/')
	bioportal.addCustomParameter("apikey", BIOPORTAL_API_KEY)

	#We're going to use CHEAR as the base context
	CHEAR_LOCATION="/home/s/projects/TWC/chear-ontology/chear.ttl"
	## This seed query will select all ChEBI classes present in CHEAR.
	seed_query = """
		PREFIX owl: <http://www.w3.org/2002/07/owl#>
		SELECT DISTINCT ?c WHERE{
			?c a owl:Class .
			FILTER(regex(str(?c), "http://purl.obolibrary.org/obo/CHEBI"))
		}
		"""
	graph = Graph()
	graph.parse(CHEAR_LOCATION,format='turtle')
	seeds = {row[0] for row in graph.query(seed_query)}

	#Setup for recursive ChEBI superclass retrieval, shallow subclass retrieval
	PREDICATES = [ #predicates we'll recursively expand paths for 
		RDF.type,
		RDFS.subClassOf,
		OWL.equivalentClass
	]
	extract_params = {
		'upstream' : True, 
		'downstream' : True, 
		'up_shallow' : True, 
		'down_shallow' : False,
	}
	graph = bioportal_retrieve_crawl_paths(
		seeds=seeds,
		bioportal=bioportal,
		properties=PREDICATES, 
		verbose=True,
		extract_params=extract_params)
	#Write out!
	graph.serialize('./data/extracts/chebi_bioportal.ttl',format='turtle')