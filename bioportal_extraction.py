

#Parameters for guiding BioPortal class retrieval
seen = set() #Classes that we've already expanded
bioportal_graph = Graph() #Where we'll expand the BioPortal results
BIOPORTAL_API_KEY = os.environ['BIOPORTAL_API_KEY']
bioportal = SPARQLWrapper('http://sparql.bioontology.org/sparql/')
bioportal.addCustomParameter("apikey", BIOPORTAL_API_KEY)



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


def find_bioportal_superclasses(k,i,verbose=False):
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


def bioportal_expand_paths(graph,seed_query):
	global bioportal_graph
	seed = _retrieve_seed_classes(seed_query)

	#Pull in BioPortal hierarchies
	print("Seeding BioPortal superclasses.")
	counter = 0
	for s in seed:
		print("=============== Class ", counter, " being expanded.")
		find_bioportal_superclasses(s,0)
		find_bioportal_subclasses(s)
		counter += 1
	print("BioPortal superclasses retrieved.")
	bio_super, bio_sub = extract_property_paths(seed, bioportal_graph, None, verbose=verbose, **extract_params)