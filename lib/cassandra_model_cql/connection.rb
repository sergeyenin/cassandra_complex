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
    # @return [CassandraModeCql::RowSet] row set
    def query(cql_string)
      row_set = RowSet.new(@conn)

      begin
        prepare_cql_statement(cql_string).each do |cql|
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

    def batch_query(cql_multi_string, options={:write_consistency=>'ANY', :write_timestamp=>nil, :read_consistency=>'QUORUM', :read_timestamp=>nil})
      raise NotImplementedError
    end

    private
    
    def prepare_cql_statement(cql_statement)
      cql_statement.gsub(/\n/, ' ').each_line(';')
    end
   
  end
end
