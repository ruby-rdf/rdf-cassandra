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
CASSANDRA_PID_FILE  = "tmp/server.pid"
CASSANDRA_HOME      = "tmp/#{CASSANDRA_DIR}"
CASSANDRA_HOST      = ENV['CASSANDRA_HOST'] || "localhost"
CASSANDRA_PORT      = ENV['CASSANDRA_PORT'] || 9160

file "tmp/#{CASSANDRA_TARBALL}" do
  sh "mkdir -p tmp"
  sh "curl #{CASSANDRA_DIST_URL} > tmp/#{CASSANDRA_TARBALL}"
end

file "tmp/#{CASSANDRA_DIR}" => "tmp/#{CASSANDRA_TARBALL}" do
  sh "tar -xzf tmp/#{CASSANDRA_TARBALL} -C tmp"
end

task :fetch   => "tmp/#{CASSANDRA_TARBALL}"
task :extract => "tmp/#{CASSANDRA_DIR}"

desc "Download and setup a local Cassandra server."
task :setup => [:fetch, :extract]

namespace :server do
  desc "Start a Cassandra server daemonized."
  task :start do
    sh "#{CASSANDRA_HOME}/bin/cassandra -p #{CASSANDRA_PID_FILE}"
  end

  desc "Stop a running Cassanda server."
  task :stop do
    abort "#{CASSANDRA_PID_FILE} does not exist, are you sure the server is running?"
    sh "kill #{File.read(CASSANDRA_PID_FILE).chomp}"
  end
end

desc "Start a Cassandra server in the foreground."
task :server do
  sh "#{CASSANDRA_HOME}/bin/cassandra -p #{CASSANDRA_PID_FILE} -f"
end

desc "Launch the Cassandra command-line client."
task :console do
  sh "#{CASSANDRA_HOME}/bin/cassandra-cli --host #{CASSANDRA_HOST} --port #{CASSANDRA_PORT}"
end
