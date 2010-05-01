module RDF::Cassandra
  ##
  # Helpers for constructing Thrift structures for the Cassandra API.
  #
  # @see http://wiki.apache.org/cassandra/API
  module Structures
    include CassandraThrift

    ##
    # @param  [Hash{Symbol => Object}] options
    # @option options [Column]         :column
    # @option options [SuperColumn]    :super_column
    # @return [ColumnOrSuperColumn]
    # @see    http://wiki.apache.org/cassandra/API#ColumnOrSuperColumn
    def column_or_supercolumn(options = {})
      options = case options
        when Column      then {:column       => options}
        when SuperColumn then {:super_column => options}
        else options
      end
      ColumnOrSuperColumn.new(options.to_hash)
    end
    alias_method :column_or_super_column, :column_or_supercolumn

    ##
    # @param  [Hash{Symbol => Object}] options
    # @option options [String, #to_s]  :name
    # @option options [String, #to_s]  :value
    # @option options [Integer, #to_i] :timestamp (Time.stamp)
    # @return [Column]
    # @see    http://wiki.apache.org/cassandra/API#Column
    def column(options = {})
      Column.new({:timestamp => Time.stamp}.merge(options.to_hash))
    end

    ##
    # @param  [Hash{Symbol => Object}] options
    # @option options [String, #to_s]  :name
    # @option options [Array<Column>]  :columns
    # @return [SuperColumn]
    # @see    http://wiki.apache.org/cassandra/API#SuperColumn
    def super_column(options = {})
      SuperColumn.new(options.to_hash).extend(SuperColumnHelpers)
    end

    ##
    # @param  [Hash{Symbol => Object}] options
    # @option options [String, #to_s]  :column_family
    # @option options [String, #to_s]  :super_column
    # @option options [String, #to_s]  :column
    # @return [ColumnPath]
    # @see    http://wiki.apache.org/cassandra/API#ColumnPath
    def column_path(options = {})
      ColumnPath.new(options.to_hash)
    end

    ##
    # @param  [Hash{Symbol => Object}] options
    # @option options [String, #to_s]  :column_family
    # @option options [String, #to_s]  :super_column
    # @return [ColumnParent]
    # @see    http://wiki.apache.org/cassandra/API#ColumnParent
    def column_parent(options = {})
      ColumnParent.new(options.to_hash)
    end

    ##
    # @param  [Hash{Symbol => Object}] options
    # @option options [Array<String>]  :column_names
    # @option options [SliceRange]     :slice_range
    # @return [SlicePredicate]
    # @see    http://wiki.apache.org/cassandra/API#SlicePredicate
    def slice_predicate(options_or_column_names)
      options = case options_or_column_names
        when Array
          {:column_names => options_or_column_names}
        when Hash
          case
            when options_or_column_names.has_key?(:column_names) ||
                 options_or_column_names.has_key?(:slice_range)
              options_or_column_names
            else
              {:slice_range => slice_range(options_or_column_names)}
          end
        else options_or_column_names
      end
      SlicePredicate.new(options.to_hash)
    end

    ##
    # @param  [Hash{Symbol => Object}] options
    # @option options [String, #to_s]  :start    ('')
    # @option options [String, #to_s]  :finish   ('')
    # @option options [Boolean]        :reversed (false)
    # @option options [Integer, #to_i] :count    (100)
    # @return [SliceRange]
    # @see    http://wiki.apache.org/cassandra/API#SliceRange
    def slice_range(options = {})
      SliceRange.new({:start => '', :finish => '', :reversed => false, :count => 100}.merge(options.to_hash))
    end

    ##
    # @param  [Hash{Symbol => Object}] options
    # @option options [String, #to_s]  :start_key   ('')
    # @option options [String, #to_s]  :end_key     ('')
    # @option options [String, #to_s]  :start_token
    # @option options [String, #to_s]  :end_token
    # @option options [Integer, #to_i] :count       (100)
    # @return [KeyRange]
    # @see    http://wiki.apache.org/cassandra/API#KeyRange
    def key_range(options = {})
      KeyRange.new({:start_key => '', :end_key => '', :count => 100}.merge(options.to_hash)) # FIXME
    end

    ##
    # @param  [Hash{Symbol => Object}] options
    # @option options [String, #to_s]              :key
    # @option options [Array<ColumnOrSuperColumn>] :columns ([])
    # @return [KeySlice]
    # @see    http://wiki.apache.org/cassandra/API#KeySlice
    def key_slice(options = {})
      KeySlice.new({:columns => []}.merge(options.to_hash))
    end

    ##
    # @param  [Hash{Symbol => Object}] options
    # @option options [String, #to_s]  :start_token
    # @option options [String, #to_s]  :end_token
    # @option options [Array<String>]  :endpoints   ([])
    # @return [TokenRange]
    # @see    http://wiki.apache.org/cassandra/API#TokenRange
    def token_range(options = {})
      TokenRange.new({:endpoints => []}.merge(options.to_hash))
    end

    ##
    # @param  [Hash{Symbol => Object}] options
    # @option options [ColumnOrSuperColumn] :column_or_supercolumn
    # @option options [Deletion]            :deletion
    # @return [Mutation]
    # @see    http://wiki.apache.org/cassandra/API#Mutation
    def mutation(options = {})
      options = case options
        when ColumnOrSuperColumn
          {:column_or_supercolumn => options}
        when Deletion
          {:deletion => options}
        else options
      end
      Mutation.new(options.to_hash)
    end

    ##
    # @param  [Hash{Symbol => Object}] options
    # @option options [Integer, #to_i] :timestamp
    # @option options [String, #to_s]  :super_column
    # @option options [SlicePredicate] :predicate
    # @return [Deletion]
    # @see    http://wiki.apache.org/cassandra/API#Deletion
    def deletion(options = {})
      Deletion.new(options.to_hash)
    end

    ##
    # @param  [Hash{Symbol => Object}] options
    # @option options [Hash{String => String}] :credentials
    # @return [AuthenticationRequest]
    # @see    http://wiki.apache.org/cassandra/API#AuthenticationRequest
    def authentication_request
      AuthenticationRequest.new(options.to_hash)
    end

    ##
    # @private
    module SuperColumnHelpers
      def has_column?(name)
        !!self[name]
      end

      def [](name)
        name = name.to_s
        columns.each do |column_or_supercolumn|
          column = column_or_supercolumn.column || column_or_supercolumn.super_column
          return column.extend(SuperColumnHelpers) if column.name.to_s == name
        end
        return nil
      end
    end # module SuperColumnHelpers
  end # module Structures
end # module RDF::Cassandra
