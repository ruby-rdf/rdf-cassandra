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
        @keyspace = ::Cassandra.new({
          options[:keyspace] || 'RDF',
          options[:servers]  || '127.0.0.1:9160',
        })

        if block_given?
          case block.arity
            when 1 then block.call(self)
            else instance_eval(&block)
          end
        end
      end
    end

    ##
    # @see RDF::Enumerable#each
    # @private
    def each(&block)
      # TODO
    end

    ##
    # @see RDF::Mutable#insert_statement
    # @private
    def insert_statement(statement)
      # TODO
    end

    ##
    # @see RDF::Mutable#delete_statement
    # @private
    def delete_statement(statement)
      # TODO
    end
  end
end
