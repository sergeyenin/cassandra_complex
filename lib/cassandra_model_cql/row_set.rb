module CassandraModelCql
  class RowSet

    include Enumerable

    attr_reader :rows, :cql_commands
    attr_reader :last_error, :last_error_command

    def initialize(conn)
      @conn = conn
      @rows = []
      @cql_commands = []
    end

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

    def each(&blck)
      @rows.each(&blck)
    end
    
    def last_command
      @cql_commands[-1]
    end
    
    private
    
    def add_rows(rws)
      return unless rws

      rws.each do |row|
        @rows.push(Row.new(row))
      end
    end
  end
end
