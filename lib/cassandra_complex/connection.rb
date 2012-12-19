require 'thread'

module CassandraComplex
  # Basic class which encapsulate driver` connection to a Cassandra cluster.
  # It executes raw CQL and can return result sets in two ways.
  #
  # @!attribute [r] keyspace
  #   @return [String] The keyspace is being connected to
  # @example Usage of Connection
  #   connection = Connection.new('127.0.0.1:9160', {:keyspace=>'cassandra_complex_test'})
  #   row_set    = connection.execute("select * from timeline")
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
      # @return [CassandraComplex::Connection] Connection instance
      def connection(kyspc=nil)
	raise MissingConfiguration if Configuration.host.nil? || Configuration.default_keyspace.nil?
        @@connections[kyspc] = CassandraComplex::Connection.new(Configuration.host, {:keyspace=>kyspc || Configuration.default_keyspace || 'system'})\
                                 unless ( @@connections[kyspc] && @@connections[kyspc].conn.active?)
        Configuration.logger.info "Connected to: #{Configuration.host}:#{Configuration.default_keyspace}"\
                                                                         if Configuration.logger.kind_of?(Logger)
        @@connections[kyspc]
      end
    end


    # Create new instance of Connection and initialize connection with Cassandra
    #
    # @param [Array, String] hosts list of hosts, a single host, to connect to
    # @param [Hash] options list of options
    # @option options [String] keyspace initial keyspace to connect, default is 'system'
    # @return [CassandraComplex::Connection] new instance
    def initialize(hosts, options = {})
      @keyspace = options[:keyspace] || 'system'
      Configuration.logger.info "Connecting to #{hosts.inspect} with params #{options.inspect}"\
                                                                  if Configuration.logger.kind_of?(Logger)
      @conn = CassandraCQL::Database.new(hosts, options.merge({:cql_version=>'3.0.0'}))
      @mutex = Mutex.new
    end

    # Execute CQL3 query with Thread safety.
    #
    # @param [Array<String>, String] cql_string string with cql3 commands
    # @param [Boolean] multi_commands if the cql_strings should be divided into separate commands
    # @param [CassandraComplex::Table] table the table with describing schema
    # @param [Array] bind bind for cql string
    # @yieldparam [Proc] blck custom code to be executed on each new row adding
    # @return [Array] row set
    def execute(cql_string, multi_commands = true, table=nil, bind=[], &blck)
      row_set = []
      @mutex.synchronize {
        begin
          join_multi_commands(cql_string, multi_commands).each do |cql|
            if !(cql.strip.empty?)
              if bind.size > 0
                cql = CassandraCQL::Statement.sanitize(cql, bind)
              end
              Configuration.logger.info "Going to execute CQL: '#{cql}'"\
                                                              if Configuration.logger.kind_of?(Logger)
              new_rows = process_thrift_rows(@conn.execute(cql), &blck)
              row_set << new_rows if new_rows
            end
          end
        ensure
          row_set_flatten = row_set.flatten
          return row_set_flatten
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

    # Execute CQL3 commands within batch
    # (see #execute)
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

    # Return key alias(first part of primary key) for given table
    #
    # @param [String] table_name Table name of given table
    # @return [String] primary key for given table
    def key_alias(table_name)
      @conn.schema.column_families[table_name].cf_def.key_alias
    end

    private

    # Process thrift rows
    #
    # @param [Array] rows thrift rows
    # @yieldparam [Proc] blck custom code to be executed on each new row adding
    def process_thrift_rows(rows, &blck)
      return unless rows
      return_value = []
      rows.fetch do |thrift_row|
        row = {}
        thrift_row.row.columns.each do |thrift_column|
          column_name  = CassandraCQL::ColumnFamily.cast(thrift_column.name, thrift_row.schema.names[thrift_column.name])
          column_value = CassandraCQL::ColumnFamily.cast(thrift_column.value, thrift_row.schema.values[thrift_column.name])
          row.merge!({column_name=>column_value})
        end
        blck.call(row) if block_given?
        return_value.push(row)
      end
      return_value
    end

    # Prepare cql statement before executing
    #
    # @param [String] cql_statement CQL3 statemenet that need to be prepared
    # @param [Boolean] multi_commands If cql_statement consist multi commands
    def join_multi_commands(cql_statement, multi_commands)
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
