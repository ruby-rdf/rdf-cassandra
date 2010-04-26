#!/usr/bin/env ruby
$:.unshift(File.expand_path(File.join(File.dirname(__FILE__), 'lib')))
require 'rubygems'
require 'rdf/cassandra'

begin
  require 'rakefile' # http://github.com/bendiken/rakefile
rescue LoadError => e
end

CASSANDRA_VERSION   = "0.6.1"
CASSANDRA_TARBALL   = "apache-cassandra-#{CASSANDRA_VERSION}-bin.tar.gz"
CASSANDRA_DIR       = "apache-cassandra-#{CASSANDRA_VERSION}"
CASSANDRA_DIST_BASE = "http://www.apache.org/dist/cassandra"
CASSANDRA_DIST_URL  = [CASSANDRA_DIST_BASE, CASSANDRA_VERSION, CASSANDRA_TARBALL].join('/')

file "tmp/#{CASSANDRA_TARBALL}" do
  sh "mkdir -p tmp"
  sh "curl #{CASSANDRA_DIST_URL} > tmp/#{CASSANDRA_TARBALL}"
end

file "tmp/#{CASSANDRA_DIR}" => "tmp/#{CASSANDRA_TARBALL}" do
  sh "tar -xzf tmp/#{CASSANDRA_TARBALL} -C tmp"
end

task :fetch   => "tmp/#{CASSANDRA_TARBALL}"
task :extract => "tmp/#{CASSANDRA_DIR}"
