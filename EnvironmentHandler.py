# -*- coding: utf-8 -*-

from abc import ABC, abstractmethod
import uuid
from rdflib import Graph
from SPARQLWrapper import SPARQLWrapper, JSON


class EnvironmentHandlerFactory:
	def createEnvironmentHander(self,environment):
		if type(environment) == Graph:
			return RDFLibGraphEnvironmentHandler(environment)
		elif type(environment) == SPARQLWrapper:
			return SPARQLWrapperEnvironmentHandler(environment)
		else:
			raise Exception("Environment type ", type(environment), 
							" is invalid; must be either rdflib.Graph.Graph or SPARQLWrapper.")

class EnvironmentHandler(ABC):
	def __init__(self,environment):
		self.environment = environment

	@abstractmethod
	def query(self,q):
		pass
	@abstractmethod
	def load_from_iri(self,iri):
		pass
	@abstractmethod
	def insert(self,graph,quad=None):
		pass


class RDFLibGraphEnvironmentHandler(EnvironmentHandler):
	def __init__(self,environment):
		#Sanity check
		assert(type(environment) == Graph)
		super(RDFLibGraphEnvironmentHandler, self).__init__(environment)

	def query(self,q):

	def load_from_iri(self,iri):
		'''
		'''
		return self.environment + Graph().parse(iri)

	def insert(self,graph):
		#Sanity check
		assert(type(graph) == Graph)
		pass



class SPARQLWrapperEnvironmentHandler(EnvironmentHandler):
	def __init__(self,environment):
		#Sanity check
		assert(type(environment) == SPARQLWrapper)
		super(SPARQLWrapperEnvironmentHandler, self).__init__(environment)
		self.compute_context = None

	def query(self,q):
		'''
		Execute generic SPARQL query,
		passed as q.
		'''
		pass

	def insert(self,graph):
		pass

	def load_from_iri(self,iri):
		'''
		Load in remote graph from given IRI,
		creating a quad where it will be stored.
		'''
		query = """
		LOAD <%s> INTO GRAPH <%s>
		""" % (iri,iri)
		self.environment.setQuery(query)
		results = self.environment.query()
		return self.environment
 
 	def drop_graph(self,iri):
 		query = """
 		DROP <%s>
 		""" % (iri,)
 		self.environment.setQuery(query)
		results = self.environment.query()
		return self.environment