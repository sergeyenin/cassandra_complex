module CassandraComplex
  # Class Table which wraps CQL3 operations
  #
  # @!attribute [rw] keyspace
  #   @return [String] The keyspace is being connected to
  # @!attribute [rw] configuration
  #   @return [String] The configuration of the connection
  # @example Selecting all rows with given primary
  #   class Timeline < CassandraComplex::Table
  #     set_table_name 'timeline'
  #   end
  #
  #   rows = Timeline.all('some_primary_key') do |row|
  #     # this puts is outputed on each row is being fetch
  #     puts row['body']
  #   end
  #   rows.each do |row|
  #     # puts is being outputed when all rows are fetched from Cassandra
  #     puts row['body']
  #   end
  class Table
    #not neccessary to allow .new
    private_class_method :new

    class << self

      attr_accessor :keyspace
      attr_accessor :configuration

      # Specify keyspace for particular table
      #
      # @param [String] kyspc keyspace
      def set_keyspace(kyspc)
        self.keyspace = kyspc
      end

      # Return connection for given keyspace(or default keyspace if no keyspace is given)
      #
      # @param [String] kyspc keyspace for which return the connection
      # @return [CassandraComplex::Connection] new instance of Connection
      def connection(kyspc=nil)
        CassandraComplex::Connection.connection(kyspc || self.keyspace)
      end

      # Execute code within specific keyspace
      #
      # @param [String] kyspc keyspace in which code should be executed
      # @yieldparam [Proc] blck custom code to be executed within keyspace
      def with_keyspace(kyspc, &blck)
        connection.with_keyspace(kyspc, &blck)
      end

      # Set table name for particular table
      #
      # @param[String] tbl table name
      def set_table_name(tbl)
        @tbl_name = tbl
      end

      # Return table name
      #
      # @return [String] table name
      def table_name
        @tbl_name || self.name.downcase
      end

      # Return first part of primary key
      #
      # @return [String] first part of primary key
      def id
        @id ||= connection.key_alias(table_name)
        @id
      end

      # Return if table is empty
      #
      # @return [Boolean] if table is empty
      def empty?
        all.empty?
      end

      # Truncate table
      # @return [Boolean] true
      def truncate
        command = "truncate #{table_name};"
        rs = connection.execute(command, true, self)
        true
      end

      # Raw query execution
      # @param [String] cql_query_string
      # @yieldparam [Proc] blck custom code to be executed within keyspace
      # @return [Array<Hash>] result set as array of hashes
      def execute(cql_query_string, &blck)
        rs = connection.execute(cql_query_string, true, self, [], &blck)
        rs
      end

      # Return result set for given primary key
      # @see .build_select_clause
      #
      # @param [String, Array<String>, Hash] key key for where clause
      # @param [Hash] clauses select clauses
      # @option clauses [String, Array<String>] where where clause
      # @option clauses [String] order order clause
      # @option clauses [String] limit limit clause
      # @return [Array<Hash>] array of hashes
      def all(key=nil, clauses={}, &blck)
        key = nil if key == :all

        return_value = nil
        if (!clauses[:select_expression])
          if (clauses[:distinct])
            clauses.merge!({:select_expression=>clauses[:distinct]})
          else
            clauses.merge!({:select_expression=>"*"})
          end
        end

        command = build_select_clause(key, clauses)
        if clauses[:where].kind_of?(Array)
          bind = clauses[:where][1..-1]
        else
          bind = []
        end
        return_value = connection.execute(command, true, self, bind, &blck)

        #distinct
        return_value = return_value.map{|row| row[clauses[:distinct]]}.uniq if clauses[:distinct]

        return_value
      end

      alias find all

      # Return count of result set for given primary key
      # @see .build_select_clause
      #
      # @param [String, Array<String>, Hash] key key for where clause
      # @param [Hash] clauses select clauses
      # @option clauses [String, Array<String>] where where clause
      # @option clauses [String] order order clause
      # @option clauses [String] limit limit clause
      # @return [Array<Hash>] result as as array of hashes
      def count(key=nil, clauses={}, &blck)
        key = nil if key == :all
        return_value = nil
        command = build_select_clause(key, clauses.merge({:select_expression=>"count(1)"}))
        if clauses[:where].kind_of?(Array)
          bind = clauses[:where][1..-1]
        else
          bind = []
        end
        rs = connection.execute(command, true, self, bind, &blck)
        if !rs.empty? && rs[0].has_key?('count')
          return_value = rs[0]['count']
        end

        return_value
      end

      # Insert record
      # @param [Hash] clauses hash of fields, which need to be inserted
      # @param [Hash] options create options, such as consistency
      # @return [Boolean] always true
      def create(clauses={}, options={})
        return false if clauses.empty?

        keys   = clauses.keys.join(', ')
        values = clauses.values.map{|x| !!options[:sanitize] ? x : CassandraCQL::Statement.quote(CassandraCQL::Statement.cast_to_cql(x))}.join(', ')

        options_clause = ''

        if !options.empty?
          options_clause = "using " + options.map{|x,y| ' ' + x.to_s + ' ' + y.to_s + ' '}.join(' AND ')
        end

        command = "insert into #{table_name} (#{keys}) values (#{values}) #{options_clause}"
        rs = connection.execute(command, true, self)

        return true
      end


      alias update create

      # Delete record(s)
      # @param [String, Array<String>, Hash] key key for where clause
      # @param [Hash] options options for delete
      # @option options [String] timestamp timestamp of operation
      # @option options [Array<String>] columns columns which should be deleted
      # @option options [Array, String] where where options for delete operation
      # @return [Boolean] always true
      def delete(key=nil,options={})
        return false unless (key || options.has_key?(:where))

        where_clause = build_where_clause(key, options)

        consistency_clause = ''
        consistency_clause = " using consistency quorum and timestamp #{options[:timestamp]} " if options[:timestamp]

        columns_clause = ''
        columns_clause = options[:columns].join(', ') if options[:columns]
        command = "delete #{columns_clause} from #{table_name} #{consistency_clause} #{where_clause}"

        if options[:where].kind_of?(Array)
          bind = options[:where][1..-1]
        else
          bind = []
        end
        rs = connection.execute(command, true, self, bind)

        return true
      end

    protected

      # Build select clause from clauses hash
      # @see .build_where_clause
      #
      # @param [String, Array<String>, Hash] key key for where clause
      # @param [Hash] clauses select clauses
      # @option clauses [String, Array<String>] where where clause
      # @option clauses [String] order order clause
      # @option clauses [String] limit limit clause
      # @return [String] CQL command
      def build_select_clause(key=nil, clauses={})
        where_clause = build_where_clause(key, clauses)
        order_clause = ''
        limit_clause = ''
        if !clauses.empty?
          order_clause = ' order by ' + clauses[:order] if clauses[:order]
          limit_clause = ' limit ' + clauses[:limit].to_s if clauses[:limit]
        end
        command = "select #{clauses[:select_expression]} from #{table_name} #{where_clause} #{order_clause} #{limit_clause};"
        command
      end

      # Build where clause from clauses hash
      #
      # @param [String, Array<String>, Hash] key key for where clause
      # @param [Hash] clauses where clauses
      # @option clauses [String, Array<String>] where where clause
      # @return [String] where part of CQL command
      def build_where_clause(key, clauses)
        where_clause = ''
        if clauses[:where].kind_of?(Array)
          where = clauses[:where][0]
        else
          where = clauses[:where]
        end
        if key
          if key.kind_of?(String)
            where_clause = "where #{id} = #{CassandraCQL::Statement.quote(CassandraCQL::Statement.cast_to_cql(key))}"
          elsif key.kind_of?(Array)
            where_clause = "where #{id} in (#{key.map{|x| CassandraCQL::Statement.quote(CassandraCQL::Statement.cast_to_cql(x))}.join(', ')})"
          elsif key.kind_of?(Hash)
            where_clause = "where " + key.map{|x,y| "#{x} = #{CassandraCQL::Statement.quote(CassandraCQL::Statement.cast_to_cql(y))}"}.join(' and ')
          end
          if !clauses.empty? && clauses[:where]
            where_clause << ' and ' + where
          end
        elsif !clauses.empty? && clauses[:where]
          where_clause = 'where ' + where
        end
        where_clause
      end

    end
  end
end
