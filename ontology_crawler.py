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
from rdflib.namespace import RDF, RDFS, OWL

from copy import deepcopy
import sys
import os

#5183781806
########## Reporting Methods #############################################################################
'''
These are only used to display information about our results. 
They can be safely removed from the code base without any 
side effects, provided that their calls are removed from main().
'''

def report_ontologies(graph):
	'''
	Print list of all ontologies now present in Graph.
	'''
	all_res = graph.query("""
		PREFIX owl: <http://www.w3.org/2002/07/owl#>
		SELECT ?ont WHERE {
			?ont a owl:Ontology.
		}
		""")
	print("Ontologies retrieved: ")
	for r in all_res:
		print(str(r[0]))
	print(len(all_res), " total.")

#TODO: Implement verbose output
def report_hierarchies(seed,graph):
	pass


########## Graph Crawling ####################################################################

def retrieve_ontologies(graph, error=None, inplace=True):
	'''
	This method will recursively crawl owl:import statements,
	starting with any statements present in graph.

	'error' defines what to do in the event of the inability
	to parse an ontology. If error=None, then the method will
	fail quickly and raise an Exception. If error='ignore',
	then the ontology will simply be ignored, as if it weren't
	the object of an owl:imports call.

	'inplace' determines what the method will return. If inplace=True,
	then the input graph will be included alongside all imported ontologies.
	If not, a graph containing only our imported ontologies will be returned.
	'''
	gout = Graph()
	seen = set()
	def _import_ontologies(g):
		'''
		Recursively work over ontology imports,
		querying for new import statements and
		adding the read data to the global graph.
		'''
		nonlocal gout
		nonlocal seen
		nonlocal error

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
			#Check whether we've already imported
			if row[0] in seen:
				continue
			else:
				seen.add(row[0])

			#Import the ontology
			FORMATS=['xml','n3','nt','trix','rdfa']
			read_success = False
			for form in FORMATS:
				try:
					#If we successfully read our ontology, recurse
					gin = Graph().parse(str(row[0]),format=form)
					#Add g to our graph
					gout = gout + gin
					read_success = True
					_import_ontologies(gin)
					break
				except Exception as e:
					pass

			#If unable to parse ontology, decide how to handle error
			if not read_success:
				print("Failed to read " + row[0] + ".")
				if error is None:
					raise Exception("Exhausted format list. Failing quickly.")
				if error == 'ignore':
					print("Exhausted format list. Quietly ignoring failure.")
				
	_import_ontologies(graph)

	#Return a graph containing our input graph and imports
	if inplace:
		return graph + gout
	#Return a graph containing just the imports
	else:
		return gout


def extract_property_paths(seeds, graph, properties,
	verbose=False,upstream=True,downstream=True,shallow=None,up_shallow=True,down_shallow=True):
	'''
	This method accepts a seed entity (either an instance or class),
	a target graph, and a list of properties. The graph is queried
	for any property paths formed by the union of the list and stemming
	from the seed instance. 'Stemming' here means to begin with any 
	triple for which the seed is either the subject or object.

	Directionality can be designated using the 'upstream' and 'downstream' 
	flags. If 'downstream' is enabled, triple chains like the following will
	be retrieved:

	seed --P1-> result --P2-> result ...

	where any P_n is a member of 'properties'.

	If 'upstream' is enabled, triple chains of the following form will
	be retrieved:

	result --P1-> result --P2-> seed

	The shallow keyword can be set to True, which will treat both
	up_shallow and down_shallow as True.
	'''

	gout = Graph()

	if shallow is not None:
		up_shallow = shallow
		down_shallow = shallow

	'''
	The following submethods are recursive solutions to 
	extracting the property paths from the input graph. 
	It's true that the upstream and downstream paths could 
	be captured more succinctly and efficiently by using a 
	SPARQL property path, like (prop_a|prop_b|...)+.
	The reason a recursive solution was chosen instead
	is because the tree structure of the hierarchy would
	be collapsed if this approach were taken, making it 
	impossible to preserve the individual edges in the graph.

	If there's a better way of doing this, go ahead and fix it!
	'''
	def __property_filter_str():
		'''
		Create a FILTER clause dictating that
		the ?prop variable is equal to one of
		our input properties.
		'''
		nonlocal properties
		return "FILTER(" + " || ".join(["?prop = <%s>" % (str(p),) for p in properties]) + ")"

	seen_downstream = set()
	def __find_downstream(entity):
		nonlocal graph
		nonlocal gout
		nonlocal up_shallow
		nonlocal seen_downstream

		#Retrieve next set of nodes in tree with edge labels
		res = graph.query("""
			PREFIX owl: <http://www.w3.org/2002/07/owl#>
			PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
			SELECT ?prop ?downstream WHERE{
				<%s> ?prop ?downstream.
				%s
			}
			""" % (str(entity),__property_filter_str()))
		for r in res:
			#Add triple to our graph
			prop = r[0]
			downstream = r[1]
			gout.add((entity,prop,downstream))
			#Skip if we've seen the node, or only want depth=1
			if down_shallow or downstream in seen_downstream:
				continue
			else:
				seen_downstream.add(downstream)
				__find_downstream(downstream)

	seen_upstream = set()
	def __find_upstream(entity):
		nonlocal graph
		nonlocal gout
		nonlocal down_shallow
		nonlocal seen_upstream

		#Retrieve next set of nodes in tree with edge labels
		res = graph.query("""
			PREFIX owl: <http://www.w3.org/2002/07/owl#>
			PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
			SELECT ?upstream ?prop WHERE {
				?upstream ?prop <%s>.
				%s
			}
			""" % (str(entity),__property_filter_str()))
		for r in res:
			#Add triple to our graph
			upstream = r[0]
			prop = r[1]
			gout.add((upstream, prop, entity))
			#Skip if we've seen the node, or only want depth=1
			if up_shallow or upstream in seen_upstream:
				continue
			else:
				seen_upstream.add(upstream)
				__find_upstream(upstream)


	#Retrieve property paths
	for seed in seeds:
		if upstream:
			__find_upstream(seed)
			seen_upstream.add(seed)
		if downstream:
			__find_downstream(seed)
			seen_downstream.add(seed)
	if verbose:
		print("Number of upstream classes retrieved: ", len(seen_upstream))
		print("Number of downstream classes retrieved: ", len(seen_downstream))

	return gout


def _retrieve_seed_classes(graph,query):
	'''
	TODO: Implement some validation to make sure
	that our query only retrieves one variable.
	'''
	return {row[0] for row in graph.query(query)}


def retrieve_crawl_paths_from_context(
	seed_graph, 
	context,
	properties,
	seed_query=None,
	expand_ontologies=True,import_error=None,verbose=False,inplace=False,
	extract_params={'upstream' : True, 'downstream' : True, 'up_shallow' : True, 'down_shallow' : True}):
	'''
	This is a wrapper method for retrieve_crawl_paths.
	The only difference is that instead of expanding
	the property paths within the main graph, the 
	context will be used instead.

	This is equivalent to manually extracting seed classes 
	from seed_graph using the custom seed_query and
	passing them alongside context to retrieve_crawl_paths().
	'''
	#Retrieve our seed classes
	if seed_query is not None:
		seeds = _retrieve_seed_classes(seed_graph,seed_query)
	else:
		raise Exception("Must pass seed query as parameter!")
	if verbose:
		print("Seeds retrieved in retrieve_crawl_paths_from_context: ", len(seeds))

	#Perform the graph crawl within the context graph
	return retrieve_crawl_paths(		
		graph=context, 
		properties=properties,
		seed_query=None,
		seeds=seeds, 
		expand_ontologies=expand_ontologies,
		import_error=import_error,
		verbose=verbose,
		inplace=inplace, 
		extract_params=extract_params)


def retrieve_crawl_paths(
	graph,
	properties,
	seed_query=None,seeds=None,
	expand_ontologies=True,import_error=None,verbose=False,inplace=False,
	extract_params={'upstream' : True, 'downstream' : True, 'up_shallow' : True, 'down_shallow' : True}):
	'''
	Method for retrieving entity paths for the given
	list of properties from the input graph, without including full ontology imports.

	seed_query is a SPARQL query designating what classes from graph 
	to use as root nodes when expanding the property paths.

	Property paths is a list of properties to follow through the graph.
	Note that all properties will be used at every level of recursion.
	For example, assume our properties are [P1,P2]. Say that A connects to B with P1,
	and that B connects to C with P2. In this case, the full path will be retrieved:

	<A> <P1> <B>.
	<B> <P2> <C>.

	If the user only wants the one property along a given path, then they should call
	this method twice: once for P1, and one for P2.

	retrieve_paths(<P1>) --> <A> <P1> <B> .
	retrieve_paths(<P2>) --> <B> <P2> <C> .

	This method is configurable, with different flags which
	will adjust its behavior:

		- expand_ontologies: whether or not to recursively pull ontologies
			referenced via owl:imports. This will recurse until
			no more ontologies can be imported.

		- inplace: If True, return a graph containing the input graph and retrieved data.
			Otherwise, return a graph containing only the retrieved property paths.


	The real value of this method is being able to walk property paths across multiple
	ontologies without needing to keep them, e.g. expand_ontologies=True. Otherwise,
	the property paths will only be retrieved for the input graph.

	If the user wishes to add the full ontology tree to their graph, including all of
	the property paths, they can instead call graph = retrieve_ontologies(graph,inplace=True) .
	'''
	#We want either a list of seeds or a seed query, not both
	if (seed_query is None and seeds is None) or (seed_query is not None and seeds is not None):
		raise Exception("seed_query and seeds are mutually exclusive parameters. Please set exactly one.")

	#Collect the initial seed of classes to expand
	if seed_query is not None and seeds is None:
		seeds = _retrieve_seed_classes(graph,seed_query)
	if verbose:
		if len(seeds) > 0:
			print("Number of seed classes: ", len(seeds))
			print("Sample classes: ")
			sample_size = 10 if len(seeds) >= 10 else len(seeds)
			for i in range(sample_size):
				print(list(seeds)[i])
		else:
			print("seed_query didn't retrieve any classes.")

	#Decide whether to pull in ontologies
	if expand_ontologies:
		ontology_graph = retrieve_ontologies(graph,inplace=False,error=import_error)
		if verbose:
			report_ontologies(ontology_graph)
	else:
		ontology_graph = Graph()

	#Extract property paths
	entity_graph = Graph()
	entity_graph += extract_property_paths(
		seeds,
		graph + ontology_graph, 
		properties, 
		#verbose=verbose, 
		**extract_params)

	#Cleanup
	del ontology_graph

	#Decide whether or not to lump everything into original graph
	if inplace:
		return graph + entity_graph
	else:
		return entity_graph



############### Command Line Interface ####################################################################


if __name__ == '__main__':
	#Predicates we're interested in expanding paths for
	PREDICATES = [ #predicates we'll recursively expand paths for 
		RDF.type,
		RDFS.subClassOf,
		OWL.equivalentClass
	]
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
	# graph = retrieve_ontologies(graph, inplace=False)
	extract_params = {
		'upstream' : True, 
		'downstream' : True, 
		'up_shallow' : False, 
		'down_shallow' : True, 
		'verbose' : True
	}
	graph = retrieve_crawl_paths(
		graph, 
		seed_query=seed_query, 
		properties=PREDICATES, 
		verbose=True, 
		inplace=True, 
		extract_params=extract_params)