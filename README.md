Apache Cassandra Storage Adapter for RDF.rb
===========================================

This is an [RDF.rb][] plugin that adds support for storing RDF data in the
[Apache Cassandra][Cassandra] distributed database management system.

* <http://github.com/bendiken/rdf-cassandra>
* <http://blog.datagraph.org/2010/04/rdf-nosql-diff>

Features
--------

* Stores RDF statements in a resource-centric manner using one Cassandra
  supercolumn family per RDF repository.
* Inherits Cassandra's characteristics of high availability, eventual
  consistency, and horizontal scalability.
* Optimized for write-heavy workloads with no need to perform a read before
  inserting or deleting an RDF statement.
* Optimized for resource-oriented access patterns to RDF statements about a
  particular subject.
* Partitions RDF data across the Cassandra cluster based on subject URIs,
  improving data locality when accessing statements about a particular
  subject.
* Includes a set of Rake tasks that make it easy to download and setup a
  local development instance of Cassandra.

Limitations
-----------

* Does not support named graphs at present.

Examples
--------

    require 'rdf/cassandra'

### Connecting to a Cassandra server running on `localhost`

    repository = RDF::Cassandra::Repository.new

### Connecting to specific Cassandra servers

    repository = RDF::Cassandra::Repository.new(:servers => "127.0.0.1:9160")

### Configuring the Cassandra keyspace and column family

    repository = RDF::Cassandra::Repository.new({
      :keyspace      => "MyApplication",  # defaults to "RDF"
      :column_family => "MyRepository",   # defaults to "Resources"
    })

Configuration
-------------

As of Cassandra 0.6, all keyspaces and column families must be predeclared
in `storage-conf.xml`. You can think of each used Cassandra supercolumn
family as being equivalent to an RDF repository, so you'll want to configure
as many as you are likely to need.

The following configuration snippet matches the default options for constructing
an `RDF::Cassandra::Repository` instance:

    <Keyspaces>
      <Keyspace Name="RDF">
        <ColumnFamily Name="Resources"
                      ColumnType="Super"
                      CompareWith="UTF8Type"
                      CompareSubcolumnsWith="BytesType"
                      Comment="RDF data."/>
      </Keyspace>
    </Keyspaces>

See `etc/storage-conf.xml` for a full configuration file example compatible
with Cassandra 0.6.

Data Model
----------

This storage adapter stores RDF data in a resource-centric manner by mapping
RDF subject terms to Cassandra row keys, RDF predicates to Cassandra
supercolumns, and RDF object terms to Cassandra columns as follows:

    {key     => {supercolumn => {column    => value }}}   # Cassandra terminology
    {subject => {predicate   => {object_id => object}}}   # RDF terminology

RDF object terms are stored using their canonical [N-Triples][]
serialization and are uniquely identified by the binary SHA-1 fingerprint of
that representation.

For example, here's how some of RDF.rb's [DOAP data][RDF.rb DOAP] would be
stored using the `RDF::Cassandra` data model:

    {
      "http://rdf.rubyforge.org/" => {
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" => {
          "c0b66f5e31ec616497404f044ff0eaa210f21232" => "<http://usefulinc.com/ns/doap#Project>",
        },
        "http://usefulinc.com/ns/doap#developer" => {
          "9d178ddaa88acfec63f812aa270b42291381b4ff" => "<http://ar.to/#self>",
          "908b42dd9d1a3f5ac5ecf9540e1f9a753f444204" => "<http://bhuga.net/#ben>",
          ...
        },
        ...
      },
      "http://ar.to/#self" => {
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" => {
          "74a5c03994aacac0a36003afb61aaf7befc438fd" => "<http://xmlns.com/foaf/0.1/Person>",
        },
        "http://xmlns.com/foaf/0.1/name" => {
          "f369f748e964ef2b82160d6389b63fb55949b464" => '"Arto Bendiken"',
        },
        ...
      },
      "http://bhuga.net/#ben" => {
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" => {
          "74a5c03994aacac0a36003afb61aaf7befc438fd" => "<http://xmlns.com/foaf/0.1/Person>",
        },
        "http://xmlns.com/foaf/0.1/name" => {
          "97325e589ac0194e74848090181b66b0db310750" => '"Ben Lavender"',
        },
        ...
      },
    }

To learn more about Cassandra's data model, read [WTF is a SuperColumn?][WTF].

Documentation
-------------

<http://rdf.rubyforge.org/cassandra/>

* {RDF::Cassandra}
  * {RDF::Cassandra::Repository}

Dependencies
------------

* [RDF.rb](http://rubygems.org/gems/rdf) (>= 0.1.9)
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

[RDF.rb]:      http://rdf.rubyforge.org/
[RDF.rb DOAP]: http://rdf.rubyforge.org/doap.ttl
[Cassandra]:   http://cassandra.apache.org/
[N-Triples]:   http://blog.datagraph.org/2010/03/grepping-ntriples
[WTF]:         http://arin.me/blog/wtf-is-a-supercolumn-cassandra-data-model
