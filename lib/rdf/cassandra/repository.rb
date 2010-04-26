module RDF::Cassandra
  ##
  # @see RDF::Repository
  class Repository < RDF::Repository
    DEFAULT_SERVERS       = '127.0.0.1:9160'
    DEFAULT_KEYSPACE      = 'RDF'
    DEFAULT_COLUMN_FAMILY = 'Resources'

    # @return [Cassandra]
    attr_reader :keyspace

    ##
    # @param  [Hash{Symbol => Object}] options
    # @option options [String, #to_s] :servers       ("127.0.0.1:9160")
    # @option options [String, #to_s] :keyspace      ("RDF")
    # @option options [String, #to_s] :column_family ("Resources")
    # @yield  [repository]
    # @yieldparam [Repository] repository
    def initialize(options = {}, &block)
      super(options) do
        @keyspace = ::Cassandra.new(
          options[:keyspace] || DEFAULT_KEYSPACE,
          options[:servers]  || DEFAULT_SERVERS
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
      @options[:column_family] || DEFAULT_COLUMN_FAMILY
    end

    ##
    # @see RDF::Enumerable#empty?
    # @private
    def empty?
      column_families.all? do |column_family|
        @keyspace.count_range(column_family).to_i.zero?
      end
    end

    ##
    # @see RDF::Enumerable#count
    # @private
    def count
      count = 0
      each_predicate do |predicate|
        each_subject do |subject|
          column_families.each do |column_family|
            count += @keyspace.count_columns(column_family, subject.to_s, predicate.to_s)
          end
        end
      end
      count
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
            if @keyspace.count_columns(column_family, slice.key.to_s).nonzero?
              block.call(RDF::Resource.new(slice.key.to_s))
            end
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
    # @see RDF::Enumerable#each_object
    # @private
    def each_object(&block)
      if block_given?
        values = {}
        column_families.each do |column_family|
          @keyspace.get_range(column_family).each do |slice|
            slice.columns.each do |column_or_supercolumn|
              column  = column_or_supercolumn.column || column_or_supercolumn.super_column
              columns = !column.respond_to?(:columns) ? [column] : column.columns
              columns.each do |column|
                value = column.value.to_s
                unless values.include?(value)
                  values[value] = true
                  block.call(RDF::NTriples.unserialize(value))
                end
              end
            end
          end
        end
      else
        enum_object
      end
    end

    ##
    # @see RDF::Enumerable#contexts
    # @private
    def contexts(options = {})
      []
    end

    ##
    # @see RDF::Enumerable#has_context?
    # @private
    def has_context?(value)
      false
    end

    ##
    # @see RDF::Enumerable#each_context
    # @private
    def each_context(&block)
      enum_context unless block_given?
    end

    ##
    # @see RDF::Enumerable#each_graph
    # @private
    def each_graph(&block)
      if block_given?
        block.call(RDF::Graph.new(nil, :data => self))
      else
        enum_graph # @since RDF.rb 0.1.9
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
      value = RDF::NTriples.serialize(statement.object)
      @keyspace.remove(column_family, statement.subject.to_s, statement.predicate.to_s, Digest::SHA1.hexdigest(value))
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
