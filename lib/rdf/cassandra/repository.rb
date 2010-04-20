module RDF::Cassandra
  ##
  # @see RDF::Repository
  class Repository < RDF::Repository
    # @return [Cassandra]
    attr_reader :keyspace

    ##
    # @param  [Hash{Symbol => Object}] options
    # @option options [String, #to_s] :keyspace ("RDF")
    # @option options [String, #to_s] :servers  ("127.0.0.1:9160")
    # @option options [String, #to_s] :family   ("RDF")
    # @yield  [repository]
    # @yieldparam [Repository] repository
    def initialize(options = {}, &block)
      super(options) do
        @keyspace = ::Cassandra.new(
          options[:keyspace] || 'RDF',
          options[:servers]  || '127.0.0.1:9160'
        )

        if block_given?
          case block.arity
            when 1 then block.call(self)
            else instance_eval(&block)
          end
        end
      end
    end

    ##
    # @return [String]
    def column_family
      @options[:family] || 'RDF'
    end

    ##
    # @see RDF::Enumerable#each
    # @private
    def each(&block)
      # TODO
    end

    ##
    # @see RDF::Enumerable#each_subject
    # @private
    def each_subject(&block)
      if block_given?
        @keyspace.get_range(column_family).each do |slice|
          block.call(RDF::Resource.new(slice.key.to_s))
        end
      else
        enum_subject
      end
    end

    ##
    # @see RDF::Enumerable#each_predicate
    # @private
    def each_predicate(&block)
      if block_given?
        values = {}
        @keyspace.get_range(column_family).each do |slice|
          slice.columns.each do |column_or_supercolumn|
            column = column_or_supercolumn.column
            value  = column.name.to_s
            unless values.include?(value)
              values[value] = true
              block.call(RDF::URI.new(value))
            end
          end
        end
      else
        enum_predicate
      end
    end

    ##
    # @see RDF::Mutable#insert_statement
    # @private
    def insert_statement(statement)
      @keyspace.insert(column_family, statement.subject.to_s, {
        statement.predicate.to_s => RDF::NTriples.serialize(statement.object)
      })
    end

    ##
    # @see RDF::Mutable#delete_statement
    # @private
    def delete_statement(statement)
      # TODO
    end
  end
end
