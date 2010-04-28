module RDF::Cassandra
  ##
  # @see RDF::Repository
  class Repository < RDF::Repository
    DEFAULT_SERVERS       = '127.0.0.1:9160'
    DEFAULT_KEYSPACE      = 'RDF'
    DEFAULT_COLUMN_FAMILY = 'Resources'
    DEFAULT_SLICE_SIZE    = 100

    # @return [Cassandra]
    attr_reader :keyspace

    ##
    # @param  [Hash{Symbol => Object}] options
    # @option options [String, #to_s]  :servers       ("127.0.0.1:9160")
    # @option options [String, #to_s]  :keyspace      ("RDF")
    # @option options [String, #to_s]  :column_family ("Resources")
    # @option options [Integer, #to_i] :slice_size    (100)
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
    # @see RDF::Durable#durable?
    # @private
    def durable?
      true
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
      # TODO: https://issues.apache.org/jira/browse/CASSANDRA-744
      count = 0
      each_key_slice do |key_slice|
        key_slice.columns.each do |column_or_supercolumn|
          column = column_or_supercolumn.column || column_or_supercolumn.super_column
          count += !column.respond_to?(:columns) ? 1 : column.columns.size
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
    # @see RDF::Enumerable#has_statement?
    # @private
    def has_statement?(statement)
      !statement.has_context? && has_triple?(statement.to_triple)
    end

    ##
    # @see RDF::Enumerable#each_statement
    # @private
    def each_statement(&block)
      if block_given?
        each_key_slice do |key_slice|
          subject = RDF::Resource.new(key_slice.key.to_s)
          key_slice.columns.each do |column_or_supercolumn|
            column    = column_or_supercolumn.column || column_or_supercolumn.super_column
            columns   = !column.respond_to?(:columns) ? [column] : column.columns
            predicate = RDF::URI.new(column.name.to_s) # TODO: use RDF::URI.intern
            columns.each do |column|
              object = RDF::NTriples.unserialize(column.value.to_s)
              block.call(RDF::Statement.new(subject, predicate, object))
            end
          end
        end
      else
        enum_statement
      end
    end

    ##
    # @see RDF::Enumerable#has_triple?
    # @private
    def has_triple?(triple)
      !query(triple).empty? # TODO: simplify this
    end

    ##
    # @see RDF::Enumerable#has_quad?
    # @private
    def has_quad?(quad)
      !quad[3] && has_triple?(quad[0...3])
    end

    ##
    # @see RDF::Enumerable#each_subject
    # @private
    def each_subject(&block)
      if block_given?
        each_key_slice do |key_slice|
          if @keyspace.count_columns(column_family, key_slice.key.to_s).nonzero?
            block.call(RDF::Resource.new(key_slice.key.to_s))
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
        each_key_slice do |key_slice|
          key_slice.columns.each do |column_or_supercolumn|
            column = column_or_supercolumn.column || column_or_supercolumn.super_column
            value  = column.name.to_s
            unless values.include?(value)
              values[value] = true
              block.call(RDF::URI.new(value)) # TODO: use RDF::URI.intern
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
        each_key_slice do |key_slice|
          key_slice.columns.each do |column_or_supercolumn|
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
    # @see RDF::Queryable#query
    # @private
    def query(pattern, &block)
      if block_given?
        case pattern
          when RDF::Statement
            query_pattern(pattern, &block)
          else super
        end
      else
        raise ArgumentError.new("expected pattern, got #{pattern.inspect}") unless pattern
        enum = Enumerator.new(self, :query, pattern)
        enum.extend(RDF::Enumerable, RDF::Queryable)
        def enum.to_a() super.extend(RDF::Enumerable, RDF::Queryable) end
        enum
      end
    end

    protected

    ##
    # @see RDF::Queryable#query_pattern
    # @private
    def query_pattern(pattern, &block)
      options = case
        when pattern.has_subject?
          {:first_key => pattern.subject.to_s, :count => 1, :slice_size => 1}
        else {}
      end

      each_key_slice(options) do |key_slice|
        subject = RDF::Resource.new(key_slice.key.to_s)
        if !pattern.has_subject? || subject == pattern.subject
          key_slice.columns.each do |column_or_supercolumn|
            column    = column_or_supercolumn.column || column_or_supercolumn.super_column
            columns   = !column.respond_to?(:columns) ? [column] : column.columns
            predicate = RDF::URI.new(column.name.to_s) # TODO: use RDF::URI.intern
            if !pattern.has_predicate? || predicate == pattern.predicate
              columns.each do |column|
                object = RDF::NTriples.unserialize(column.value.to_s)
                if !pattern.has_object? || object == pattern.object
                  block.call(RDF::Statement.new(subject, predicate, object))
                end
              end
            end
          end
        end
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
        statement.predicate.to_s => {sha1(value) => value}
      })
    end

    ##
    # @see RDF::Mutable#delete_statement
    # @private
    def delete_statement(statement)
      value = RDF::NTriples.serialize(statement.object)
      @keyspace.remove(column_family, statement.subject.to_s, statement.predicate.to_s, sha1(value))
    end

    ##
    # @see RDF::Mutable#clear_statements
    # @private
    def clear_statements
      column_families.each do |column_family|
        each_key_slice do |key_slice|
          @keyspace.remove(column_family, key_slice.key)
        end
        # FIXME: this is buggy in Cassandra/Ruby 0.8.2, deleting only 100 rows at a time:
        #@keyspace.clear_column_family!(column_family)
      end
    end

    ##
    # @return [Integer]
    # @private
    def slice_size
      @options[:slice_size] || DEFAULT_SLICE_SIZE
    end

    ##
    # @private
    def each_key_slice(options = {}, &block)
      if block_given?
        column_families.each do |column_family|
          first_key  = options[:first_key]
          start_key  = nil
          count      = options[:count] || nil
          slice_size = options[:slice_size] || self.slice_size
          loop do
            key_slices = @keyspace.get_range(column_family, :start => start_key || first_key, :count => slice_size)
            key_slices.shift if start_key # start key is inclusive
            break if key_slices.empty?

            if count
              key_slices.each do |key_slice|
                block.call(key_slice)
                return if (count -= 1).zero?
              end
            else
              key_slices.each(&block)
            end

            start_key = key_slices.last.key
          end
        end
      else
        Enumerator.new(self, :each_key_slice, options)
      end
    end

    ##
    # @return [String]
    # @private
    def sha1(data)
      Digest::SHA1.digest(data)
    end
  end
end
