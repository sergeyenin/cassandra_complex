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
      conn = CassandraCQL::Database.new(hosts, options.merge({:cql_version=>'3.0.0'}))
    end

    # Execute CQL3 query within connection
    # @param [Array, String] cql_strings string with cql3 commands
    # @return [CassandraModeCql::RowSet] row set
    def query(cql_strings)
      row_sets = []
      cql_string.each_line do |cql|
        row_sets << conn.execute(cql)
      end
      row_sets.flatten
    end

    def with_keyspace(kyspc)
      raise NotImplementedError
    end

    def batch_query(cql_multi_string)
      raise NotImplementedError
    end
  end
end
