# -*- coding: utf-8 -*-

from abc import ABC, abstractmethod
import uuid
from rdflib import Graph
from SPARQLWrapper import SPARQLWrapper, JSON


class EnvironmentHandlerFactory:
	'''
	Simple Factory class designed to handle different types
	of input Environments and return appropriate EnvironmentHandlers
	that can be generically manipulated in other segments of the 
	code base with no loss of functionality. For now, the list of
	valid input Environments for the EnvironmentHandlerFactory
	are as follows:
	- rdflib.graph.Graph
	- SPARQLWrapper.SPARQLWrapper
	'''
	def createEnvironmentHander(self,environment):
		TYPE_MAP = {
			Graph : RDFLibGraphEnvironmentHandler,
			SPARQLWrapper : SPARQLWrapperEnvironmentHandler
		}
		if not type(environment) in VALID_TYPES.keys():
			raise Exception("Environment type ", type(environment), 
							" is invalid; must be either rdflib.Graph.Graph or SPARQLWrapper.")
		else:
			return TYPE_MAP[type(environment)](environment=environment)


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
		pass

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