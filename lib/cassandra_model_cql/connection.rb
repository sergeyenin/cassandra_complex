require 'thread'

module CassandraModelCql
  # Basic class which encapsulate driver` connection to a Cassandra cluster.
  # It executes raw CQL and can return result sets in two ways.
  #
  # @!attribute [r] keyspace
  #   @return [String] The keyspace is being connected to
  # @example Usage of Connection
  #   connection = Connection.new('127.0.0.1:9160')
  #   row_set    = connection.execute("select * timeline")
  #   row_set.each |row|
  #     puts row['user_id']
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
      def connection(kyspc=nil)
	raise MissingConfiguration if Configuration.host.nil? || Configuration.keyspace.nil?
        @@connections[kyspc] = CassandraModelCql::Connection.new(Configuration.host, {:keyspace=>kyspc || Configuration.default_keyspace || 'system'})\
                                 unless ( @@connections[kyspc] && @@connections[kyspc].conn.active?)
        @@connections[kyspc]
      end
    end


    # Create new instance of Connection and initialize connection with Cassandra
    #
    # @param [Array, String] hosts list of hosts, a single host, to connect to
    # @param [Hash] options list of options
    # @option options [String] keyspace initial keyspace to connect; defaults to 'system'
    # @return [CassandraModelCql::Connection] new instance
    def initialize(hosts, options = {})
      @keyspace = options[:keyspace] || 'system'
      @conn = CassandraCQL::Database.new(hosts, options.merge({:cql_version=>'3.0.0'}))
      @mutex = Mutex.new
    end

    # Execute CQL3 query with Thread safety.
    #
    # @param [Array, String] cql_string string with cql3 commands
    # @param [Boolean] multi_commands if the cql_strings should be divided into separate commands
    # @param [CassandraModelCql::Table] table the table with describing schema
    # @return [Array] row set
    def execute(cql_string, multi_commands = true, table=nil, &blck)
      row_set = []

      @mutex.synchronize {
        begin
          prepare_cql_statement(cql_string, multi_commands).each do |cql|
            if !(cql.strip.empty?)
              new_rows = add_rows(@conn.execute(cql), &blck)
              row_set << new_rows if new_rows
            end
          end
        ensure
          return row_set
        end
      }
    end

    # Change current keyspace temporarily; restore original keyspace upon return.
    #
    # @param [String] kyspc The keyspace of chaning context
    # @yield Execute cassandra operations within context of kyspc
    def with_keyspace(kyspc)
      @mutex.synchronize {
        if kyspc != @keyspace.strip
          old_keyspace, @keyspace = @keyspace, kyspc

          execute("use #{@keyspace};")
          yield if block_given?
          execute("use #{old_keyspace};")

          @keyspace = old_keyspace
        else
          yield if block_given?
        end
      }
    end

    # Execute CQL3 commands within batch, @see execute
    #
    # @param [String, Array] cql_commands CQL3 commands to be executed within batch
    # @param [Hash] options Consistency options of batch command
    # @option options[String] :write_consistency ('ANY') Write consistency
    # @option options[Time]   :write_timestamp   (nil)   Write timestamp
    # @option options[String] :read_consistency ('QUORUM') Read consistency
    # @option options[Time]   :read_timestamp   (nil)   Read timestamp
    # @return [CassandraModeCql::RowSet] row set
    def execute_batch(cql_commands, options={:write_consistency=>'ANY', :write_timestamp=>nil, :read_consistency=>'QUORUM', :read_timestamp=>nil})
      command = "\
        BEGIN BATCH #{prepare_consistency_level(options)}
          #{cql_commands}
        APPLY BATCH;\
      "
      execute(command, false)
    end

    # Return key alias(primary key) for given table
    #
    # @param [String] table_name Table name of given table
    # @return [String] primary key for given table
    def key_alias(table_name)
      @conn.schema.column_families[table_name].cf_def.key_alias
    end

    private

    def add_rows(rows, &blck)
      return unless rows

      rows.fetch do |thrift_row|
        row = {}
        thrift_row.row.columns.each do |thrift_column|
          column_name  = CassandraCQL::ColumnFamily.cast(thrift_column.name, thrift_row.schema.names[thrift_column.name])
          column_value = CassandraCQL::ColumnFamily.cast(thrift_column.value, thrift_row.schema.values[thrift_column.name])
          row.merge!({column_name=>column_value})
        end
        blck.call(row) if block_given?
        @rows.push(row)
      end
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
