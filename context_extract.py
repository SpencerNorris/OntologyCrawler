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
from rdflib.util import guess_format
from rdflib.namespace import RDFS, OWL
import javaproperties

#Home cookin' modules
from ontology_crawler import retrieve_crawl_paths_from_context



def extract_from_contexts(
	seed_iri,properties,
	property_f,
	extract_params,
	dest_dir,
	verbose=False,error=None):
	'''
	Loads in our seed graph from the provided IRI.
	Then iterates through our provided Java property
	file, which contains prefixes and IRIs for
	context ontologies. Entities with the associated
	prefix are pulled from the seed ontology via the seed query.
	These seed entities are then used as roots for the property 
	crawl paths in the associated context ontology. 
	'''
	seed_graph = Graph().parse(seed_iri)
	if verbose:
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
	if verbose:
		print("Read properties file.")

	#Iterate through rows of properties file
	for k in javaprops.keys():
		if verbose:
			print("==========================================")
			print("Reading graph: ", str(k))
		#Pull the ontology IRI and associated prefix
		row = javaprops[k].split(',')
		prefix = row[0]
		iri = row[2]
		#Check whether we have an IRI for the row
		if iri == '':
			if verbose:
				print("No IRI provided.")
			continue
		if iri == seed_iri:
			if verbose:
				print("Context graph is same as seed graph. Skipping.")
			continue

		#Read in graph from IRI, use as context
		context = Graph()
		FORMATS=['xml','n3','nt','trix','turtle','rdfa']
		read_success = False
		for form in FORMATS:
			try:
				#If we successfully read our ontology, recurse
				context = Graph().parse(iri,format=form)
				read_success = True
				if verbose:
					print("Read as ", form, ".")
				break
			except Exception as e:
				pass
		if not read_success:
			#Last-ditch effort: try to guess the file extension
			try:
				if verbose:
					print("Exhausted format list. Attempting to guess format...")
				context = Graph().parse(iri,format=guess_format(iri))
				if verbose:
					print("Read as ", guess_format(iri), ".")
				break
			except Exception as e:
				pass
			#Error handling, quiet or fast failing
			if error is None:
				raise Exception("Exhausted format list. Failing quickly.")
			if error == 'ignore':
				print("Exhausted format list. Quietly ignoring failure.")
				continue

		#Expand property paths from context
		gout = retrieve_crawl_paths_from_context(	
			seed_graph=seed_graph, 
			context=context,
			properties=properties,
			seed_query=SEED_QUERY_TEMPLATE % (prefix,),
			expand_ontologies=True,
			import_error=error,
			verbose=verbose,
			inplace=False,
			extract_params=extract_params)

		#Write out
		gout.serialize(dest_dir + k + '.ttl',format='turtle')
		print("Wrote out " + dest_dir + k + '.ttl.')
		del gout

		# try:
		# 	#Attempt to write out our extracted property paths the canonical way
		# 	gout.serialize(dest_dir + k + '.ttl',format='turtle')
		# except Exception as e:
		# 	#Sometimes rdflib barfs when we try to write out.
		# 	#In this case, open a file and manually write out our triples
		# 	print(e)
		# 	print("Serialization failed. attempting iterative hack.")
		# 	with open(dest_dir + k + '.ttl','w') as fout:
		# 		for s,p,o in gout.triples((None,None,None)):
		# 			#Try something!



if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument(
		'-f', '--file', type=str,
		nargs='?', default=None,
		help="Java properties file to read in. Must be set by user."
	)
	parser.add_argument(
		'-d', '--dest', type=str,
		nargs='?', default='./data/extracts/',
		help="Directory to which we want to write out our extracted property path graphs."
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
		extract_params=extract_params,
		dest_dir=args.dest,
		error='ignore')
