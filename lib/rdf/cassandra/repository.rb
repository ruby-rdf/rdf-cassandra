module RDF::Cassandra
  ##
  # @see RDF::Repository
  class Repository < RDF::Repository
    # @return [Cassandra]
    attr_reader :keyspace

    ##
    # @param  [Hash{Symbol => Object}] options
    # @option options [String, #to_s] :keyspace      ("RDF")
    # @option options [String, #to_s] :servers       ("127.0.0.1:9160")
    # @option options [String, #to_s] :column_family ("RDF")
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
    # @return [Array<String>]
    def column_families
      [column_family]
    end

    ##
    # @return [String]
    def column_family
      @options[:column_family] || 'RDF'
    end

    ##
    # @see RDF::Enumerable#each
    # @private
    def each(&block)
      each_statement(&block)
    end

    ##
    # @see RDF::Enumerable#each_statement
    # @private
    def each_statement(&block)
      if block_given?
        column_families.each do |column_family|
          @keyspace.get_range(column_family).each do |slice|
            subject = RDF::Resource.new(slice.key.to_s)
            slice.columns.each do |column_or_supercolumn|
              column    = column_or_supercolumn.column || column_or_supercolumn.super_column
              columns   = !column.respond_to?(:columns) ? [column] : column.columns
              predicate = RDF::URI.new(column.name.to_s)
              columns.each do |column|
                object = RDF::NTriples.unserialize(column.value.to_s)
                block.call(RDF::Statement.new(subject, predicate, object))
              end
            end
          end
        end
      else
        enum_statement
      end
    end

    ##
    # @see RDF::Enumerable#each_subject
    # @private
    def each_subject(&block)
      if block_given?
        column_families.each do |column_family|
          @keyspace.get_range(column_family).each do |slice|
            block.call(RDF::Resource.new(slice.key.to_s))
          end
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
        column_families.each do |column_family|
          @keyspace.get_range(column_family).each do |slice|
            slice.columns.each do |column_or_supercolumn|
              column = column_or_supercolumn.column || column_or_supercolumn.super_column
              value  = column.name.to_s
              unless values.include?(value)
                values[value] = true
                block.call(RDF::URI.new(value))
              end
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
      # {keyspace => {column_family => {key     => {supercolumn => {column    => value}}}}}
      # {keyspace => {column_family => {subject => {predicate   => {object_id => object}}}}}
      value = RDF::NTriples.serialize(statement.object)
      @keyspace.insert(column_family, statement.subject.to_s, {
        statement.predicate.to_s => {Digest::SHA1.hexdigest(value) => value}
      })
    end

    ##
    # @see RDF::Mutable#delete_statement
    # @private
    def delete_statement(statement)
      # TODO
    end

    ##
    # @see RDF::Mutable#clear_statements
    # @private
    def clear_statements
      column_families.each do |column_family|
        @keyspace.clear_column_family!(column_family)
      end
    end
  end
end
