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

      def all(key=nil, clauses={}, &blck)

        where_clause = ''
        if key
          where_clause = "WHERE #{id} = '#{key}'"
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

        command = "SELECT * from #{table_name} #{where_clause} #{order_clause}"
        rs = connection.query(command, true, self, &blck)
        self.last_error, self.last_error_command = rs.last_error, rs.last_error_command
        rs.rows || {}
      end

    end

  end
end
