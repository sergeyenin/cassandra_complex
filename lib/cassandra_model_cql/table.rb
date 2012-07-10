module CassandraModelCql
  # Class Table which wraps CQL3 operations
  #
  # @example Selecting all rows with given primary
  #   class Timeline < CassandraModelCql::Table
  #   end
  #
  #   rows = Timeline.all('some_primary_key')
  #   rows.each do |row|
  #     puts row['body']
  #   end
  class Table
    @id, @keyspace = nil, 'system'
    @last_error, @last_error_command = nil, nil

    #not neccessary to allow .new
    private_class_method :new

    class << self
      attr_accessor :last_error, :last_error_command
      attr_accessor :keyspace

      def set_keyspace(kyspc)
        self.keyspace = kyspc
      end

      def connection(kyspc=nil)
        CassandraModelCql::Connection.connection(kyspc || self.keyspace)
      end

      def with_keyspace(kyspc, &blck)
        connection.with_keyspace(kyspc, &blck)
      end

      def table_name
        self.name.downcase
      end

      def id
        @id ||= connection.conn.schema.column_families[table_name].cf_def.key_alias
        @id
      end

      #raw query
      def query(cql_query_string, &blck)
        rs = connection.query(cql_query_string, true, self, &blck)
        self.last_error, self.last_error_command = rs.last_error, rs.last_error_command
        rs
      end

      def all(key=nil, clauses={}, &blck)
        command = build_select_clause(key, clauses.merge({:select_expression=>"*"}))
        rs = connection.query(command, true, self, &blck)
        self.last_error, self.last_error_command = rs.last_error, rs.last_error_command
        rs.rows || []
      end

      alias find all

      def count(key=nil, clauses={}, &blck)
        command = build_select_clause(key, clauses.merge({:select_expression=>"count(1)"}))
        rs = connection.query(command, true, self, &blck)
        self.last_error, self.last_error_command = rs.last_error, rs.last_error_command
        rs.rows || []
      end

      def create(clauses={}, options={})
        return false if clauses.empty?

        keys   = clauses.keys.join(', ')
        values = clauses.values.join(', ')

        timestamp_clause = ''
        timestamp_clause = "using timestamp #{options[:timestamp]}" if options[:timestamp]

        command = "insert into #{table_name} (#{keys}) values (#{values}) #{timestamp_clause}"

        rs = connection.query(command, true, self)
        self.last_error, self.last_error_command = rs.last_error, rs.last_error_command

        return (self.last_error  == nil)
      end

      alias update create

      def delete(key=nil,options={})
        return false unless key

        where_clause = ''
        if key.kind_of?(Array)
          where_clause = " where #{id} in (#{key.join(', ')})"
        elsif key.kind_of?(String)
          where_clause = " where #{id} = #{key}"
        else
          return false
        end

        columns_clause = ''
        columns_clause = options[:columns].join(', ') if options[:columns]

        command = "delete #{columns_clause} from #{table_name} #{where_clause}"
        rs = connection.query(command, true, self)
        self.last_error, self.last_error_command = rs.last_error, rs.last_error_command

        return (self.last_error  == nil)
      end

    private

      def build_select_clause(key=nil, clauses={})
        where_clause = ''
        if key
          where_clause = "where #{id} = '#{key}'"
          if !clauses.empty? && clauses[:where]
            where_clause << ' and ' + clauses[:where]
          end
        elsif !clauses.empty? && clauses[:where]
          where_clause = 'where ' + clauses[:where]
        end

        order_clause = ''
        if !clauses.empty? && clauses[:order]
          order_clause = ' order by ' + clauses[:order]
        end
        command = "select #{clauses[:select_expression]} from #{table_name} #{where_clause} #{order_clause}"
        command
      end
    end
  end
end
