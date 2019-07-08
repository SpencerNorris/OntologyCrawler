

#Parameters for guiding BioPortal class retrieval
seen = set() #Classes that we've already expanded
bioportal_graph = Graph() #Where we'll expand the BioPortal results




# def report_bioportal():
# 	global seen
# 	global bioportal_graph
# 	#TODO: Expand with more information on 
# 	print("Number of BioPortal superclasses: ", len(seen))

########## BioPortal Graph Crawling ####################################################################



import json
import urllib
from urllib.parse import urlencode, quote_plus
import traceback

import ontology_crawler


def extract_bioportal_property_paths(seeds,bioportal,predicates,downstream=True,upstream=True,verbose=False):
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
	seen = set()
	bioportal_graph = Graph()

	def _query_bioportal_downstream(k,bioportal,predicates):
		'''
		Retrieve the next level downstream of the predicate path
		using the class k. Return a dictionary mapping
		from the retrieved class names to the properties
		by which they connect to k.
		'''
		#Construct filter and query BioPortal for paths
		filter_str = "FILTER(" + " || ".join(["?pred = <%s>" % (pred,) for pred in predicates]) + ")"
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

	def _crawl_bioportal_downstream(k,i):
		nonlocal seen_downstream
		nonlocal verbose
		nonlocal bioportal
		nonlocal predicates
		nonlocal bioportal_graph


		if verbose:
			print("Recursion level: ", i)
			print("Node: ", str(k))

		#If we've already expanded this node, don't recurse
		if str(k) in seen:
			if verbose:
				print("Already seen!")
			return
		else:				
			#Note that we're about to expand the parent
			if verbose:
				print("Not seen, expanding...")
			seen.add(k)

		#Retrieve all parents, properties for connecting back to input class
		parents = _query_bioportal_downstream(k)
		#Go over all of the classes that were retrieved, if any
		for k_n in parents.keys():
			#Expand our next node
			_crawl_bioportal_downstream(k_n,i+1)


	for k in seeds:
		if downstream:
			_crawl_bioportal_downstream(k,0)
		#TODO: Implement!
		# if upstream:
		# 	_crawl_bioportal_upstream(k,0)
	return bioportal_graph


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
	seed = ontology_crawler._retrieve_seed_classes(seed_query)

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




if __name__ == '__main__':
	#Adapted from https://github.com/ncbo/sparql-code-examples/blob/master/python/sparql1.py
	BIOPORTAL_API_KEY = os.environ['BIOPORTAL_API_KEY']
	bioportal = SPARQLWrapper('http://sparql.bioontology.org/sparql/')
	bioportal.addCustomParameter("apikey", BIOPORTAL_API_KEY)