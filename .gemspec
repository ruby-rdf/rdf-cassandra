#!/usr/bin/env ruby -rubygems
# -*- encoding: utf-8 -*-

GEMSPEC = Gem::Specification.new do |gem|
  gem.version            = File.read('VERSION').chomp
  gem.date               = File.mtime('VERSION').strftime('%Y-%m-%d')

  gem.name               = 'rdf-cassandra'
  gem.homepage           = 'http://rdf.rubyforge.org/cassandra/'
  gem.license            = 'Public Domain' if gem.respond_to?(:license=)
  gem.summary            = 'Apache Cassandra adapter for RDF.rb.'
  gem.description        = 'RDF.rb plugin providing an Apache Cassandra storage adapter.'
  gem.rubyforge_project  = 'rdf'

  gem.authors            = ['Arto Bendiken']
  gem.email              = 'arto.bendiken@gmail.com'

  gem.platform           = Gem::Platform::RUBY
  gem.files              = %w(AUTHORS README UNLICENSE VERSION etc/log4j.properties etc/storage-conf.xml) + Dir.glob('lib/**/*.rb')
  gem.bindir             = %q(bin)
  gem.executables        = %w()
  gem.default_executable = gem.executables.first
  gem.require_paths      = %w(lib)
  gem.extensions         = %w()
  gem.test_files         = %w()
  gem.has_rdoc           = false

  gem.required_ruby_version      = '>= 1.8.7'
  gem.requirements               = ['Cassandra (>= 0.6.0)']
  gem.add_development_dependency 'rdf-spec',    '~> 0.2.0'
  gem.add_development_dependency 'rspec',       '>= 1.3.0'
  gem.add_development_dependency 'yard' ,       '>= 0.5.5'
  gem.add_runtime_dependency     'rdf',         '~> 0.2.0'
  gem.add_runtime_dependency     'cassandra',   '>= 0.8.2'
  gem.post_install_message       = nil
end
