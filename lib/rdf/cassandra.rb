require 'digest'
require 'rdf'
require 'cassandra'

module RDF
  module Cassandra
    autoload :Repository, 'rdf/cassandra/repository'
    autoload :VERSION,    'rdf/cassandra/version'
  end
end
