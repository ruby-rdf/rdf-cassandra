module RDF::Cassandra
  ##
  class Client
    include Structures

    DEFAULT_SLICE_SIZE        = 100
    DEFAULT_CONSISTENCY_LEVEL = 1

    ##
    def initialize(keyspace, options = {})
      @keyspace = keyspace
      @options  = options.dup
    end

    ##
    # @return [Integer]
    def slice_size
      (@options[:slice_size] || DEFAULT_SLICE_SIZE).to_i
    end

    ##
    # @return [Integer]
    def consistency_level
      (@options[:consistency_level] || DEFAULT_CONSISTENCY_LEVEL).to_i
    end

    ##
    def each_key_slice(column_family, options = {}, &block)
      if block_given?
        options    = options.dup
        first_key  = options.delete(:first_key)
        start_key  = nil
        count      = options.delete(:count)
        slice_size = options.delete(:slice_size) || self.slice_size

        loop do
          key_slices = get_range_slices({
            :parent    => column_parent(:column_family => column_family.to_s, :super_column => options[:super_column]),
            :predicate => options[:predicate] || slice_predicate(:start => options[:start_column] || '', :finish => options[:end_column] || '', :count => options[:column_count] || 1_000),
            :range     => key_range(:start_key => (start_key || first_key).to_s, :end_key => '', :count => slice_size),
          })

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
      else
        RDF::Enumerator.new(self, :each_key_slice, column_family, options)
      end
    end

    ##
    # @see http://wiki.apache.org/cassandra/API#get
    def get(options = {})
      client.get(
        options[:keyspace] || @keyspace.keyspace.to_s,
        options[:key],
        options[:path] || column_path({
          :column_family => options[:column_family],
          :super_column  => options[:super_column],
          :column        => options[:column],
        }),
        options[:consistency] || consistency_level)
    end

    ##
    # @see http://wiki.apache.org/cassandra/API#get_slice
    def get_slice(options = {})
      client.get_slice(
        options[:keyspace] || @keyspace.keyspace.to_s,
        options[:key],
        options[:parent],
        options[:predicate],
        options[:consistency] || consistency_level)
    end

    ##
    # @see http://wiki.apache.org/cassandra/API#get_count
    def get_count(options = {})
      client.get_count(
        options[:keyspace] || @keyspace.keyspace.to_s,
        options[:key],
        options[:parent],
        options[:consistency] || consistency_level)
    end

    ##
    # @see http://wiki.apache.org/cassandra/API#get_range_slices
    def get_range_slices(options = {})
      client.get_range_slices(
        options[:keyspace] || @keyspace.keyspace.to_s,
        options[:parent],
        options[:predicate],
        options[:range],
        options[:consistency] || consistency_level)
    end

    ##
    # @see http://wiki.apache.org/cassandra/API#batch_mutate
    def batch_mutate(options = {})
      client.batch_mutate(
        options[:keyspace] || @keyspace.keyspace.to_s,
        options[:mutation_map],
        options[:consistency] || consistency_level)
    end

    ##
    # @param  [Hash{String => Hash}]   data
    # @param  [Hash{Symbol => Object}] options
    # @return [void]
    def insert_data(data, options = {})
      timestamp = Time.stamp
      mutations = {}
      data.each do |column_family, rows|
        rows.each do |key, supercolumns|
          mutations[key] ||= {}
          mutations[key][column_family] ||= []
          supercolumns.each do |supercolumn, columns|
            mutations[key][column_family] << mutation(column_or_supercolumn(super_column({
              :name    => supercolumn,
              :columns => columns.map do |column, value|
                column(:name => column, :value => value, :timestamp => timestamp)
              end
            })))
          end
        end
      end
      batch_mutate(options.merge(:mutation_map => mutations))
    end

    ##
    # @return [CassandraThrift::Cassandra::Client]
    # @private
    def client
      @keyspace.send(:client)
    end
  end # class Client
end # module RDF::Cassandra
