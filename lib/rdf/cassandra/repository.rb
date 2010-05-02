module RDF::Cassandra
  ##
  # @see RDF::Repository
  class Repository < RDF::Repository
    include Structures

    DEFAULT_SERVERS       = '127.0.0.1:9160'
    DEFAULT_KEYSPACE      = :RDF
    DEFAULT_COLUMN_FAMILY = :Resources
    DEFAULT_INDEX_FAMILY  = :Index
    INSERT_BATCH_SIZE     = 100
    DELETE_BATCH_SIZE     = 100

    # @return [Cassandra]
    attr_reader :keyspace

    ##
    # @param  [Hash{Symbol => Object}] options
    # @option options [String, #to_s]  :servers       ("127.0.0.1:9160")
    # @option options [String, #to_s]  :keyspace      (:RDF)
    # @option options [String, #to_s]  :column_family (:Resources)
    # @option options [String, #to_s]  :index_family  (:Index)
    # @option options [Integer, #to_i] :slice_size    (100)
    # @yield  [repository]
    # @yieldparam [Repository] repository
    def initialize(options = {}, &block)
      super(options) do
        @keyspace = ::Cassandra.new(
          (options[:keyspace] || DEFAULT_KEYSPACE).to_s,
          (options[:servers]  || DEFAULT_SERVERS)
        )
        @client = Client.new(keyspace, options)

        if block_given?
          case block.arity
            when 1 then block.call(self)
            else instance_eval(&block)
          end
        end
      end
    end

    ##
    # @return [Enumerable<String>]
    def column_families
      [column_family]
    end

    ##
    # @return [String]
    def column_family
      @options[:column_family] || DEFAULT_COLUMN_FAMILY
    end

    ##
    # @return [Boolean]
    def indexed?
      @options[:indexed] == true
    end

    ##
    # @param  [Symbol, #to_sym]
    # @return [Boolean]
    def has_index?(type)
      indexed? && [:ps, :os, :op].include?(type.to_sym)
    end

    ##
    # @return [void]
    def index!
      index_statements(self)
    end

    ##
    # @see RDF::Repository#supports?
    # @private
    def supports?(feature)
      case feature.to_sym
        # We do *not* support contexts / named graphs at this time:
        when :context then false
        else super
      end
    end

    ##
    # @see RDF::Durable#durable?
    # @private
    def durable?
      true
    end

    ##
    # Returns `true` if this repository contains no RDF statements.
    #
    # Since Cassandra row keys stick around even after they've been deleted,
    # this can be an expensive operation in the worst case where the
    # repository contains mostly or only deleted resources. If you're
    # satisfied with a probabilistic answer as to whether the repository is
    # empty, pass in an integer value to the `:sample` option to indicate
    # the maximum number of resources (i.e. row keys) to be examined.
    #
    # @param  [Hash{Symbol => Object}] options
    # @option options [Integer, #to_i] :sample (nil)
    # @return [Boolean]
    def empty?(options = {})
      column_families.all? do |column_family|
        key_count  = !options[:sample] ? nil : (options[:sample].to_i rescue 1_000)
        key_slices = @client.each_key_slice(column_family, :count => key_count, :column_count => 1)
        key_slices.all? { |key_slice| key_slice.columns.empty? }
      end
    end

    ##
    # @see RDF::Enumerable#count
    # @private
    def count
      # TODO: https://issues.apache.org/jira/browse/CASSANDRA-744
      count = 0
      column_families.each do |column_family|
        @client.each_key_slice(column_family) do |key_slice|
          key_slice.columns.each do |column_or_supercolumn|
            column = column_or_supercolumn.column || column_or_supercolumn.super_column
            count += !column.respond_to?(:columns) ? 1 : column.columns.size
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
        column_families.each do |column_family|
          @client.each_key_slice(column_family) do |key_slice|
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
        end
      else
        enum_statement
      end
    end

    ##
    # @see RDF::Enumerable#has_quad?
    # @private
    def has_quad?(quad)
      !quad[3] && has_triple?(quad[0...3])
    end

    ##
    # @see RDF::Enumerable#has_triple?
    # @private
    def has_triple?(triple)
      !query(triple).empty? # TODO: simplify this
    end

    ##
    # @see RDF::Enumerable#has_subject?
    # @private
    def has_subject?(value)
      column_families.any? do |column_family|
        begin
          slice = @client.get_slice({
            :key       => value.to_s,
            :parent    => @client.column_parent(:column_family => column_family.to_s),
            :predicate => @client.slice_predicate(:start => '', :finish => '', :count => 1),
          })
          !slice.empty?
        rescue CassandraThrift::NotFoundException => e
          false
        end
      end
    end

    ##
    # @see RDF::Enumerable#each_subject
    # @private
    def each_subject(&block)
      case
        when !block_given?
          enum_subject
        else
          column_families.each do |column_family|
            @client.each_key_slice(column_family) do |key_slice|
              if @keyspace.count_columns(column_family, key_slice.key.to_s).nonzero?
                block.call(RDF::Resource.new(key_slice.key.to_s))
              end
            end
          end
      end
    end

    ##
    # @see RDF::Enumerable#has_predicate?
    # @private
    def has_predicate?(value)
      case
        when has_index?(:ps)
          begin
            !!@client.get({
              :key           => sha1(value, :binary => false),
              :column_family => index_family(:p).to_s,
              :super_column  => :info.to_s,
            })
          rescue CassandraThrift::NotFoundException => e
            false
          end
        else
          super # TODO: optimize this
      end
    end

    ##
    # @see RDF::Enumerable#each_predicate
    # @private
    def each_predicate(&block)
      case
        when !block_given?
          enum_predicate
        when has_index?(:ps)
          each_predicate_indexed(&block)
        else
          each_predicate_unindexed(&block)
      end
    end

    ##
    # @private
    def each_predicate_indexed(&block)
      values = {}
      @client.each_key_slice(index_family(:p), :super_column => 'ps', :column_count => 1) do |key_slice|
        unless key_slice.columns.empty?
          result = @client.get({
            :key           => key_slice.key.to_s,
            :column_family => index_family(:p).to_s,
            :super_column  => :info.to_s,
            :column        => [key_slice.key.to_s].pack('H*'), # FIXME after Cassandra 0.7
          })
          value = result.column.value.to_s
          unless values.include?(value)
            values[value] = true
            block.call(RDF::NTriples.unserialize(value)) # TODO: use RDF::URI.intern
          end
        end
      end
    end

    ##
    # @private
    def each_predicate_unindexed(&block)
      values = {}
      column_families.each do |column_family|
        @client.each_key_slice(column_family) do |key_slice|
          key_slice.columns.each do |column_or_supercolumn|
            column = column_or_supercolumn.column || column_or_supercolumn.super_column
            value  = column.name.to_s
            unless values.include?(value)
              values[value] = true
              block.call(RDF::URI.new(value)) # TODO: use RDF::URI.intern
            end
          end
        end
      end
    end

    ##
    # @see RDF::Enumerable#has_object?
    # @private
    def has_object?(value)
      case
        when has_index?(:os)
          begin
            !!@client.get({
              :key           => sha1(value, :binary => false),
              :column_family => index_family(:o).to_s,
              :super_column  => :info.to_s,
            })
          rescue CassandraThrift::NotFoundException => e
            false
          end
        else
          super # TODO: optimize this
      end
    end

    ##
    # @see RDF::Enumerable#each_object
    # @private
    def each_object(&block)
      case
        when !block_given?
          enum_object
        when has_index?(:os)
          each_object_indexed(&block)
        else
          each_object_unindexed(&block)
      end
    end

    ##
    # @private
    def each_object_indexed(&block)
      values = {}
      @client.each_key_slice(index_family(:o), :super_column => 'os', :column_count => 1) do |key_slice|
        unless key_slice.columns.empty?
          result = @client.get({
            :key           => key_slice.key.to_s,
            :column_family => index_family(:o).to_s,
            :super_column  => :info.to_s,
            :column        => [key_slice.key.to_s].pack('H*'), # FIXME after Cassandra 0.7
          })
          value = result.column.value.to_s
          unless values.include?(value)
            values[value] = true
            block.call(RDF::NTriples.unserialize(value))
          end
        end
      end
    end

    ##
    # @private
    def each_object_unindexed(&block)
      values = {}
      column_families.each do |column_family|
        @client.each_key_slice(column_family) do |key_slice|
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
        enum = RDF::Enumerator.new(self, :query, pattern)
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

      column_families.each do |column_family|
        @client.each_key_slice(column_family, options) do |key_slice|
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
    end

    ##
    # @see RDF::Mutable#insert_statements
    # @private
    def insert_statements(statements)
      count   = 0
      inserts = {}
      statements = RDF::Enumerator.new(statements, statements.respond_to?(:each_statement) ? :each_statement : :each)
      statements.each do |statement|
        value  = RDF::NTriples.serialize(statement.object)
        insert = (inserts[statement.subject.to_s]  ||= {})
        insert = (insert[statement.predicate.to_s] ||= {})
        insert[sha1(value)] = value
        if ((count += 1) % INSERT_BATCH_SIZE).zero?
          @client.insert_data(column_family.to_s => inserts)
          inserts = {}
        end
        index_statement(statement) if indexed? # FIXME
      end
      @client.insert_data(column_family.to_s => inserts) unless inserts.empty?
      count
    end

    ##
    # @see RDF::Mutable#delete_statement
    # @private
    def delete_statement(statement)
      @keyspace.remove(column_family, statement.subject.to_s,
        statement.predicate.to_s, sha1(statement.object))
      unindex_statement(statement) if indexed?
    end

    ##
    # @see RDF::Mutable#clear_statements
    # @private
    def clear_statements
      column_families.each do |column_family|
        @client.each_key_slice(column_family) do |key_slice|
          @keyspace.remove(column_family, key_slice.key)
        end
      end
      clear_indexes
    end

    ##
    # @return [void]
    # @private
    def clear_indexes
      index_families.each do |index_family|
        @client.each_key_slice(index_family) do |key_slice|
          @keyspace.remove(index_family, key_slice.key)
        end
      end
    end

    ##
    # @return [void]
    # @private
    def index_statements(statements)
      statements = RDF::Enumerator.new(statements, statements.respond_to?(:each_statement) ? :each_statement : :each)
      statements.each do |statement|
        index_statement(statement)
      end
    end

    ##
    # @param  [RDF::Statement] statement
    # @return [void]
    # @private
    def index_statement(statement)
      index_statement_predicate(statement)
      index_statement_object(statement)
    end

    ##
    # @param  [RDF::Statement] statement
    # @return [void]
    # @private
    def unindex_statement(statement)
      unindex_statement_object(statement)
      unindex_statement_predicate(statement)
    end

    ##
    # @param  [RDF::Statement] statement
    # @return [void]
    # @private
    def index_statement_predicate(statement)
      if has_index?(:ps)
        @keyspace.insert(index_family(:p), sha1(statement.predicate, :binary => false), {
          'info' => sha1_column(statement.predicate),
          'ps'   => sha1_column(statement.subject),
        })
      end
    end

    ##
    # @param  [RDF::Statement] statement
    # @return [void]
    # @private
    def unindex_statement_predicate(statement)
      if has_index?(:ps)
        key = sha1(statement.predicate, :binary => false)
        subjects = @keyspace.get(index_family(:p), key, 'ps')
        subjects.each do |subject_id, subject|
          subject = RDF::NTriples.unserialize(subject)
          #if query(:subject => subject, :predicate => statement.predicate).empty?
          if column_families.all? { |column_family| @keyspace.get(column_family, subject.to_s, statement.predicate.to_s).empty? }
            @keyspace.remove(index_family(:p), key, 'ps', sha1(subject))
          end
        end
      end
    end

    ##
    # @param  [RDF::Statement] statement
    # @return [void]
    # @private
    def index_statement_object(statement)
      case
        when has_index?(:os) && has_index?(:op)
          @keyspace.insert(index_family(:o), sha1(statement.object, :binary => false), {
            'info' => sha1_column(statement.object),
            'os'   => sha1_column(statement.subject),
            'op'   => sha1_column(statement.predicate),
          })
        when has_index?(:os)
          # TODO
        when has_index?(:op)
          # TODO
      end
    end

    ##
    # @param  [RDF::Statement] statement
    # @return [void]
    # @private
    def unindex_statement_object(statement)
      if has_index?(:os)
        key = sha1(statement.object, :binary => false)
        objects = @keyspace.get(index_family(:o), key, 'os')
        objects.each do |subject_id, subject|
          subject = RDF::NTriples.unserialize(subject)
          if query(:object => statement.object, :subject => subject).empty?
            @keyspace.remove(index_family(:o), key, 'os', sha1(subject))
          end
        end
      end

      if has_index?(:op)
        key = sha1(statement.object, :binary => false)
        objects = @keyspace.get(index_family(:o), key, 'op')
        objects.each do |predicate_id, predicate|
          predicate = RDF::NTriples.unserialize(predicate)
          if query(:object => statement.object, :predicate => predicate).empty?
            @keyspace.remove(index_family(:o), key, 'op', sha1(predicate))
          end
        end
      end
    end

    ##
    # @return [Enumerable<Symbol>]
    # @private
    def index_families
      [index_family(:p), index_family(:o)].compact.uniq
    end

    ##
    # @param  [Symbol, #to_sym] type
    # @return [Symbol]
    # @private
    def index_family(type)
      @options[:index_family] || DEFAULT_INDEX_FAMILY # TODO
    end

    ##
    # @return [Hash{String => String}]
    # @private
    def sha1_column(value, options = {})
      if data = RDF::NTriples.serialize(value)
        {Digest::SHA1.send(options[:binary] == false ? :hexdigest : :digest, data) => data}
      end
    end

    ##
    # @return [String]
    # @private
    def sha1(value, options = {})
      value = value.is_a?(RDF::Value) ? RDF::NTriples.serialize(value) : value
      Digest::SHA1.send(options[:binary] == false ? :hexdigest : :digest, value)
    end
  end # class Repository
end # module RDF::Cassandra
