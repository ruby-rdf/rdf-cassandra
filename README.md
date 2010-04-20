Apache Cassandra Storage Adapter for RDF.rb
===========================================

This is an [RDF.rb][] plugin that adds support for storing RDF data in the
[Apache Cassandra][Cassandra] distributed database management system.

* <http://github.com/bendiken/rdf-cassandra>

Features
--------

* Stores RDF statements in Apache Cassandra in a resource-centric manner.

Limitations
-----------

* Does not support named graphs at present.

Examples
--------

    require 'rdf/cassandra'

Documentation
-------------

<http://rdf.rubyforge.org/cassandra/>

* {RDF::Cassandra}
  * {RDF::Cassandra::Repository}

Dependencies
------------

* [RDF.rb](http://rubygems.org/gems/rdf) (>= 0.1.8)
* [Cassandra for Ruby](http://rubygems.org/gems/cassandra) (>= 0.8.2)
* [Cassandra][] (>= 0.6.0)

Installation
------------

The recommended installation method is via [RubyGems](http://rubygems.org/).
To install the latest official release of the `RDF::Cassandra` gem, do:

    % [sudo] gem install rdf-cassandra

Download
--------

To get a local working copy of the development repository, do:

    % git clone git://github.com/bendiken/rdf-cassandra.git

Alternatively, you can download the latest development version as a tarball
as follows:

    % wget http://github.com/bendiken/rdf-cassandra/tarball/master

Authors
-------

* [Arto Bendiken](mailto:arto.bendiken@gmail.com) - <http://ar.to/>

License
-------

`RDF::Cassandra` is free and unencumbered public domain software. For more
information, see <http://unlicense.org/> or the accompanying UNLICENSE file.

[RDF.rb]:    http://rdf.rubyforge.org/
[Cassandra]: http://cassandra.apache.org/
