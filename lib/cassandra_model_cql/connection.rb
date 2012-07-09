require 'thread'

module CassandraModelCql
  # Basic class which encapsulate driver` connection
  #
  # @!attribute [r] keyspace
  #   @return [String] The keyspace is being connected to
  # @example Usage of Connection
  #   connection = Connection.new('127.0.0.1:9160')
  #   row_set    = connection.query("select * timeline")
  #   row_set.each |row|
  #     puts row[:user_id]
  #   end
  class Connection

    attr_reader :keyspace
    attr_reader :conn
    # Connections pool, @see .connection
    @@connections = {}

    class << self

      # Create( if not exists or not active) and return connection to kyspc
      #
      # @param [String] kyspc ('system') The keyspace to which connect
      # @return [CassandraModelCql::Connection] Connection instance
      def connection(kyspc='system')
        @@connections[kyspc] = CassandraModelCql::Connection.new('127.0.0.1:9160', {:keyspace=>kyspc})\
                                 unless ( @@connections[kyspc] && @@connections[kyspc].conn.active?)
        @@connections[kyspc]
      end
    end


    # Create new instance of Connection and initialize connection with Cassandra
    #
    # @param [Array, String] hosts list of hosts or just obvious host String
    # @param [Hash] options list of options
    # @option options [String] :keyspace The keyspace to connect
    # @return [CassandraModelCql::Connection] new instance
    def initialize(hosts, options = {})
      @keyspace = options[:keyspace] || 'system'
      @conn = CassandraCQL::Database.new(hosts, options.merge({:cql_version=>'3.0.0'}))
      @mutex = Mutex.new
    end

    # Execute CQL3 query
    #
    # @param [Array, String] cql_string string with cql3 commands
    # @param [Boolean] multi_commands if the cql_strings should be divided into separate commands
    # @param [CassandraModelCql::Table] table the table with describing schema
    # @return [CassandraModeCql::RowSet] row set
    def query(cql_string, multi_commands = true, table=nil, &blck)
      row_set = RowSet.new(@conn, table)

      @mutex.synchronize {
        begin
          prepare_cql_statement(cql_string, multi_commands).each do |cql|
            row_set << row_set.execute_query(cql, &blck) unless cql.strip.empty?
          end
        ensure
          return row_set
        end
      }
    end

    # Change context of connection temporary
    #
    # @param [String] kyspc The keyspace of chaning context
    # @yield Execute cassandra operations within context of kyspc
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

    # Execute CQL3 commands within batch, @see query
    #
    # @param [String, Array] cql_commands CQL3 commands to be executed within batch
    # @param [Hash] options Consistency options of batch command
    # @option options[String] :write_consistency ('ANY') Write consistency
    # @option options[Time]   :write_timestamp   (nil)   Write timestamp
    # @option options[String] :read_consistency ('QUORUM') Read consistency
    # @option options[Time]   :read_timestamp   (nil)   Read timestamp
    # @return [CassandraModeCql::RowSet] row set
    def batch_query(cql_commands, options={:write_consistency=>'ANY', :write_timestamp=>nil, :read_consistency=>'QUORUM', :read_timestamp=>nil})
      command = "\
        BEGIN BATCH #{prepare_consistency_level(options)}
          #{cql_commands}
        APPLY BATCH;\
      "
      query(command, false)
    end

    private

    # Prepare consistency level clause
    # @param [Hash] options Consistency level options
    # @return[String] consistency level clause
    def prepare_consistency_level(options)
      return_value = 'USING CONSISTENCY QUORUM'
      return return_value
    end

    # Prepare cql statement
    #
    # @param [String] cql_statement CQL3 statemenet that need to be prepared
    # @param [Boolean] multi_commands If cql_statement consist multi commands
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

  end
end
