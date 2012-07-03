module CassandraModelCql
  class Table
    @primary_key = nil
    class << self

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
        rs.rows || {}
      end

    end

  end
end
