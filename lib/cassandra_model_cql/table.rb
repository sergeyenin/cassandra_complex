module CassandraModelCql
  class Table
    @primary_key = nil
    @last_error, @last_error_command = nil, nil

    class << self
      attr_accessor :last_error, :last_error_command

      def connection(kyspc='history')
        CassandraModelCql::Connection.connection(kyspc)
      end

      def table_name
        self.name.downcase
      end

      def primary_key
        @primary_key ||= connection.conn.schema.column_families[table_name].cf_def.key_alias
        @primary_key
      end

      def all(key=nil)
        where_clause = ""
        where_clause = "WHERE #{primary_key} = #{key}" if key

        command = "SELECT * from #{table_name} #{where_clause}"

        rs = connection.query(command, true, self)
        self.last_error, self.last_error_command = rs.last_error, rs.last_error_command if rs.last_error
        rs.rows || {}
      end

    end

  end
end
