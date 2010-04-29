require File.join(File.dirname(__FILE__), 'spec_helper')
require 'rdf/spec/repository'

describe RDF::Cassandra::Repository do
  before :each do
    @repository = RDF::Cassandra::Repository.new(:indexed => false)
    @repository.clear
  end

  # @see lib/rdf/spec/repository.rb
  it_should_behave_like RDF_Repository
end
