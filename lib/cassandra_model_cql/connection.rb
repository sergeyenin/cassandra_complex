module CassandraModelCql
  class Connection

    attr_accessor :conn
    attr_accessor :keyspace

    # Create new instance of Connection and initialize connection with Cassandra
    #
    # @param [Array, String] hosts list of hosts or just obvious host String
    # @param [Hash] options list of options
    # @option options [String] :keyspace keyspace to which connect
    # @return [CassandraModelCql::Connection] new instance
    def initialize(hosts, options = {})
      @keyspace = options[:keyspace] || 'system'
      @conn = CassandraCQL::Database.new(hosts, options.merge({:cql_version=>'3.0.0'}))
    end

    # Execute CQL3 query within connection
    # @param [Array, String] cql_strings string with cql3 commands
    # @param [Boolean] multi_commands if the cql_strings should be divided into separate commands
    # @return [CassandraModeCql::RowSet] row set
    def query(cql_string, multi_commands = true, table=nil)
      row_set = RowSet.new(@conn, table)

      begin
        prepare_cql_statement(cql_string, multi_commands).each do |cql|
          row_set << row_set.execute_query(cql) unless cql.strip.empty?
        end
      ensure
        return row_set
      end
    end

    def with_keyspace(kyspc)
      if kyspc != @keyspace.strip
        old_keyspace, @keyspace = @keyspace, kyspc

        query("use #{@keyspace};")
        yield if block_given?
        query("use #{old_keyspace};")

        @keyspace = old_keyspace
      else
        yield if block_given?
      end
    end

    def batch_query(cql_commands, options={:write_consistency=>'ANY', :write_timestamp=>nil, :read_consistency=>'QUORUM', :read_timestamp=>nil})
      command = "\
        BEGIN BATCH #{prepare_consistency_level(options)}
          #{cql_commands}
        APPLY BATCH;\
      "
      query(command, false)
    end

    private

    def prepare_consistency_level(opts)
      return_value = 'USING CONSISTENCY QUORUM'
      return return_value
    end

    def prepare_cql_statement(cql_statement, multi_commands)
      return_value = cql_statement
      if multi_commands
        return_value  = return_value.gsub(/\n/, ' ')
        return_value  = return_value.each_line(';')
      else
        return_value  = return_value.to_a
      end
      return_value
    end

    @@connections = {}

    class << self
      def connection(kyspc='system')
        @@connections[kyspc] = CassandraModelCql::Connection.new('127.0.0.1:9160', {:keyspace=>kyspc})\
                                 unless ( @@connections[kyspc] && @@connections[kyspc].conn.active?)
        return @@connections[kyspc]
      end
    end
  end
end
