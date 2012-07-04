module CassandraModelCql
  # RowSet encapsulates any operation result
  #
  # includes "Enumerable"
  # @!parse include Enumerable
  #
  # @!attribute [r] rows
  #   @return [Array] Array of rows
  # @!attribute [r] cql_commands
  #   @return [Array] Array of cql commands which returned those RowSet
  # @!attribute [r] last_error
  #   @return [Exception] Exception that was caused by cql_commands if any
  # @!attribute [r] last_error_command
  #   @return [String] Comand that caused last_error
  class RowSet

    include Enumerable

    attr_reader :rows, :cql_commands
    attr_reader :last_error, :last_error_command

    # Create new instance of RowSet
    #
    # @param [CassandraModelCql::Connection] conn Connection which create current RowSet
    # @param [CassandraModelCql::Table] table (nil) Table which consist RowSet
    def initialize(conn, table=nil)
      @table = table
      @conn = conn
      @rows = []
      @cql_commands = []
    end

    # Execute query and fill RowSet with rows,
    # rescue any Exception
    #
    # @param [String] cql_command CQL3 command that executed
    def execute_query(cql_command)
      @cql_commands.push(cql_command)
      begin
        add_rows(@conn.execute(cql_command))
        @last_error = nil
        @last_error_command = nil
      rescue Exception => ex
        @last_error = ex
        @last_error_command = cql_command
      end
    end

    # Each as it require by Enumerable
    #
    # @yield block that invoked on rows.each pass
    # @return [Enumerator] Enumerator within Array
    def each(&blck)
      @rows.each(&blck)
    end

    # @return [String] last cql command
    def last_command
      @cql_commands[-1]
    end

    private

    # Add rows to current RowSet
    #
    # @param [CassandraCQL::Row] rws Rows that should be added to RowSet
    def add_rows(rws)
      return unless rws

      rws.fetch do |row|
        @rows.push(Row.new({:thrift_row=>row, :table=>@table}))
      end
    end

  end
end
