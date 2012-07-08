module CassandraModelCql

  # Class Table which implement Cassandra` ColumnFamily
  #
  # @example Selecting all rows with given primary
  #   class Timeline < CassandraModelCql::Table
  #   end
  #
  #   rows = Timeline.all('some_primary_key')
  #   rows.each do |row|
  #     row['body'] = 'Another body!'
  #     row.save
  #   end
  class Table
    @id, @keyspace = nil, 'system'
    @last_error, @last_error_command = nil, nil

    class << self
      attr_accessor :last_error, :last_error_command
      attr_accessor :keyspace
 
      def set_keyspace(kyspc)
        self.keyspace = kyspc
      end

      def connection(kyspc=nil)
        CassandraModelCql::Connection.connection(kyspc || self.keyspace)
      end

      def table_name
        self.name.downcase
      end

      def id
        @id ||= connection.conn.schema.column_families[table_name].cf_def.key_alias
        @id
      end

      def all(key=nil)
        where_clause = ""
        where_clause = "WHERE #{id} = #{key}" if key

        command = "SELECT * from #{table_name} #{where_clause}"

        rs = connection.query(command, true, self)
        self.last_error, self.last_error_command = rs.last_error, rs.last_error_command if rs.last_error
        rs.rows || {}
      end

    end

  end
end
