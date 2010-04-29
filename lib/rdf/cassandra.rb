require 'digest'
require 'rdf'
require 'rdf/ntriples'
require 'cassandra'

module RDF
  module Cassandra
    autoload :Client,     'rdf/cassandra/client'
    autoload :Repository, 'rdf/cassandra/repository'
    autoload :VERSION,    'rdf/cassandra/version'
  end
end
