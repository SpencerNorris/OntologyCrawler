#!/usr/bin/env python3

'''
File: pipeline.py
Author: Spencer Norris

Description: This pipeline will recursively retrieve class
hierarchies for a collection of classes, crawling ontology
import statements to expand the class hierarchy as far as
possible.

First, ontologies are recursively pulled in, using the base
ontology as the root node. owl:imports statements are crawled
and each ontology is parsed into a separate graph. This graph
is then queried for owl:imports statements and added to the
global graph, and so forth until there are no more owl:imports
statements to be read.

Next, the graph is queried to retrieve class hierarchies from the imports.
Given that all classes inherit from owl:Thing, the recursion
across the class hierarchies should theoretically result in
a directed acyclic graph with one sink node (owl:Thing). However,
since it is possible that cycles can be formed between class
hierarchies, the 'seen' set is used to track nodes in the
graph that have already been traversed, preventing infinite recursion.

The recursive class retrieval is replicated for BioPortal, less the
local ontology imports; BioPortal is simply too big to pull into
memory. Each class is expanded for paths using the PREDICATES 
provided. The relation (predicate) between the input class (subject) 
and the connected class (object) is then added to a local graph.
'''
#TODO: Retrofit so that a SPARQL endpoint, such as Blazegraph,
# can be used to store results.

from rdflib import Graph, URIRef
from rdflib.namespace import RDFS, OWL
from SPARQLWrapper import SPARQLWrapper, JSON, XML, N3, RDF
import sys
import os

########## Globals ###############################################

## This seed query will select all ChEBI classes present in CHEAR.
SEED_QUERY=	"""
		PREFIX owl: <http://www.w3.org/2002/07/owl#>
		SELECT DISTINCT ?c WHERE{
			?c a owl:Class .
			FILTER(regex(str(?c), "http://purl.obolibrary.org/obo/CHEBI"))
		}
		"""

## Parameters for managing local graph
CHEAR_LOCATION="/home/s/projects/TWC/chear-ontology/chear.ttl"
graph = Graph() #Where we'll expand our ontologies

#Parameters for guiding BioPortal class retrieval
seen = set() #Classes that we've already expanded
bioportal_graph = Graph() #Where we'll expand the BioPortal results
BIOPORTAL_API_KEY = os.environ['BIOPORTAL_API_KEY']
bioportal = SPARQLWrapper('http://sparql.bioontology.org/sparql/')
bioportal.addCustomParameter("apikey", BIOPORTAL_API_KEY)

#Predicates we're interested in expanding paths for
PREDICATES = [ #predicates we'll recursively expand paths for 
	RDFS.subClassOf,
	OWL.equivalentClass
]


########## Reporting Methods #############################################################################
'''
These are only used to display information about our results. 
They can be safely removed from the code base without any 
side effects, provided that their calls are removed from main().
'''

def show_ontologies():
	'''
	Print list of all ontologies now present in Graph.
	'''
	global graph
	all_res = graph.query("""
		PREFIX owl: <http://www.w3.org/2002/07/owl#>
		SELECT ?ont WHERE {
			?ont a owl:Ontology.
		}
		""")
	import_res = graph.query("""
		PREFIX owl: <http://www.w3.org/2002/07/owl#>
		SELECT ?ont WHERE {
			?ont a owl:Ontology.
			[] owl:imports ?ont .
		}
		""")
	print("All ontologies: ")
	for r in all_res:
		print(str(r[0]))
	print(len(all_res), " total.")


# def report_bioportal():
# 	global seen
# 	global bioportal_graph
# 	#TODO: Expand with more information on 
# 	print("Number of BioPortal superclasses: ", len(seen))


########## BioPortal Graph Crawling ####################################################################

#Adapted from https://github.com/ncbo/sparql-code-examples/blob/master/python/sparql1.py

import json
import urllib
from urllib.parse import urlencode, quote_plus
import traceback


def find_bioportal_superclasses(k,i):
	'''
	This is a recursive method that will move all
	the way up the inheritance tree until we hit 
	superclass bedrock for the class we've been given.
	The recursion should follow the path of a directed
	acyclic graph, with one sink node, owl:Thing.

	It's possible that we wind up with cycles; in order
	to deal with this, we're going to maintain a set
	containing all of our classes which we've already
	expanded, called 'klasses'.

	params:
	k --> our class that we want to expand
	klasses --> set of classes already expanded.
	'''
	global seen
	global bioportal_graph
	global PREDICATES

	def _query_next_level(k):
		'''
		Retrieve the next level up of the predicate path
		using the class k.
		'''
		global PREDICATES
		global bioportal

		#Construct filter so that we only retrieve predicates we're interested in 
		filter_str = "FILTER(" + " || ".join(["?pred = <%s>" % (pred,) for pred in PREDICATES]) + ")"
		query = """
		SELECT DISTINCT ?pred ?kn WHERE {
			<%s> ?pred ?kn.
			%s
		}
		""" % (str(k),filter_str)
		bioportal.setQuery(query)
		bioportal.setReturnFormat(JSON)
		results = bioportal.query().convert()

		#Construct dictionary mapping superclasses to lists of connecting properties
		final = {}
		for result in results['results']['bindings']:
			if not result['kn']['value'] in final.keys():
				final[URIRef(result['kn']['value'])] = []
			final[URIRef(result['kn']['value'])].append(URIRef(result['pred']['value']))
		return final

	print("Recursion level: ", i)
	print("Node: ", str(k))

	#If we've already expanded this node, don't recurse
	if str(k) in seen:
		print("Already seen!")
		return
	else:				
		#Note that we're about to expand the parent
		print("Not seen, expanding...")
		seen.add(str(k))

	#Retrieve all parents, properties for connecting back to input class
	parents = _query_next_level(k)
	#Go over all of the classes that were retrieved, if any
	for k_n in parents.keys():
		#Add property connections to graph
		for pred in parents[k_n]:
			bioportal_graph.add((k,pred,k_n))
		#Expand our next node
		find_bioportal_superclasses(k_n,i+1)


def find_bioportal_subclasses(k):
	global bioportal
	global bioportal_graph
	global PREDICATES

	#Construct query with filter to select only predicates we're interested in
	filter_str = "FILTER(" + " || ".join(["?pred = <%s>" % (pred,) for pred in PREDICATES]) + ")"
	query = """
		PREFIX owl: <http://www.w3.org/2002/07/owl#>
		PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
		SELECT ?pred ?sub WHERE {
			?sub ?pred <%s>.
			%s
		}
		""" % (str(k),filter_str)
	res = bioportal.setQuery(query)
	bioportal.setReturnFormat(JSON)
	results = bioportal.query().convert()
	#Dump results into BioPortal graph 
	for result in results['results']['bindings']:
		bioportal_graph.add( (URIRef(result['sub']['value']), URIRef(result['pred']['value']), k) )


########## Local Graph Crawling ####################################################################

#TODO: Modify so that only one local graph is necessary
def import_ontologies(g):
	'''
	Recursively work over ontology imports,
	querying for new import statements and
	adding the read data to the global graph.
	'''
	global graph

	#Add g to our graph
	graph = graph + g

	#Retrieve all ontology imports
	imports = g.query("""
		PREFIX owl: <http://www.w3.org/2002/07/owl#>
		SELECT ?ont WHERE{
		[] a owl:Ontology ; 
		   owl:imports ?ont .
		}
		""")

	#Attempt to read in ontologies
	for row in imports:
		FORMATS=['xml','n3','nt','trix','rdfa']
		read_success = False
		for form in FORMATS:
			try:
				#If we successfully read our ontology, recurse
				gin = Graph().parse(str(row[0]),format=form)
				read_success = True
				import_ontologies(gin)
				break
			except Exception as e:
				pass
				# print(e)
		if not read_success:
			print("Exhausted format list.")
			sys.exit(1)

def retrieve_enrichment_classes(seed, g):
	'''
	We're going to pull in all of the classes that are
	accessible via a local import. That means any classes
	that are present in CHEAR, any of the ontologies it
	imports, and any ontologies further up that chain.
	'''

	#TODO: Modify SPARQL queries so that our list of predicates are used instead.
	def _find_subclasses(k,g):
		'''
		We're only interested in finding depth-one subclasses.
		Shouldn't be too difficult
		'''
		res = g.query("""
			PREFIX owl: <http://www.w3.org/2002/07/owl#>
			PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
			SELECT ?sub WHERE {
				?sub (rdfs:subClassOf|owl:equivalentClass) <%s>.
			}
			""" % (str(k),))
		return [r[0] for r in res]


	#Retrieve superclass hierarchy for all seed classes
	superclasses = set()
	for s in seed:
		s_res = g.query("""
			PREFIX owl: <http://www.w3.org/2002/07/owl#>
			PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
			SELECT ?class WHERE{
				<%s> (rdfs:subClassOf|owl:equivalentClass)+ ?class.
			}
			""" % (str(s),))
		for r in s_res:
			superclasses.add(r[0])
	print("Full hierarchy size: ", len(set.union(seed, superclasses)))
	print("Intersection of seed classes and extracted hierarchy: ", 
		len(set.intersection(set(seed), superclasses))
	)
	print("Number of non-seed hierarchy classes: ",
		len(superclasses) - len(set.intersection(seed, superclasses))
	)

	#Retrieve our subclasses
	subclasses = {elem for elem in _find_subclasses(s,g) for s in seed}
	print("Total immediate subclasses: ", len(subclasses))

	return superclasses, subclasses



############### Main Section ####################################################################

def _retrieve_seed_classes(query):
	'''
	TODO: Implement some validation to make sure
	that our query only retrieves one variable.
	'''
	global graph
	return {row[0] for row in graph.query(query)}


def main():
	#Pull in CHEAR and full hierarchy of ontology imports
	global CHEAR_LOCATION
	global SEED_QUERY
	global graph
	global seen
	global bioportal_graph

	#Collect the initial seed of classes to expand
	graph.parse(CHEAR_LOCATION,format='turtle')
	seed = _retrieve_seed_classes(SEED_QUERY)
	print("Number of seed classes: ", len(seed))
	print("Sample classes: ")
	for i in range(10):
		print(list(seed)[i])
	# seen.update(seed)

	#Pull in ontology hierarchy, local class hierarchy
	import_ontologies(graph)
	show_ontologies()
	local_super, local_sub = retrieve_enrichment_classes(seed,graph)

	#Pull in BioPortal hierarchies
	print("Seeding BioPortal superclasses.")
	counter = 0
	for s in seed:
		print("=============== Class ", counter, " being expanded.")
		find_bioportal_superclasses(s,0)
		find_bioportal_subclasses(s)
		counter += 1
	print("BioPortal superclasses retrieved.")
	bio_super, bio_sub = retrieve_enrichment_classes(seed, bioportal_graph)


def expand(base_url, other_url):
	pass

if __name__ == '__main__':
	main()