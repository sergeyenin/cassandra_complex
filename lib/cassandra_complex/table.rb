module CassandraComplex
  # Class Table which wraps CQL3 operations
  #
  # @example Selecting all rows with given primary
  #   class Timeline < CassandraComplex::Table
  #   end
  #
  #   rows = Timeline.all('some_primary_key') do |row|
  #     # this puts is outputed on each row is being fetch
  #     puts row['body']
  #   end
  #   rows.each do |row|
  #     # puts is being outputed when all rows are fetched from Cassandra
  #     puts row['body']
  #   end
  class Table
    #not neccessary to allow .new
    private_class_method :new

    class << self
      attr_accessor :keyspace
      attr_accessor :configuration

      def set_keyspace(kyspc)
        self.keyspace = kyspc
      end

      def connection(kyspc=nil)
        CassandraComplex::Connection.connection(kyspc || self.keyspace)
      end

      def with_keyspace(kyspc, &blck)
        connection.with_keyspace(kyspc, &blck)
      end

      def set_table_name(tbl)
        @tbl_name = tbl
      end

      def table_name
        @tbl_name || self.name.downcase
      end

      def id
        @id ||= connection.key_alias(table_name)
        @id
      end

      def empty?
        all.empty?
      end

      def truncate
        command = "truncate #{table_name};"
        rs = connection.execute(command, true, self)
        true
      end

      #raw query execution
      def execute(cql_query_string, &blck)
        rs = connection.execute(cql_query_string, true, self, [], &blck)
        rs
      end

      def all(key=nil, clauses={}, &blck)
        key = nil if key == :all

        return_value = nil

        if (!clauses[:select_expression])
          if (clauses[:distinct])
            clauses.merge!({:select_expression=>clauses[:distinct]})
          else
            clauses.merge!({:select_expression=>"*"})
          end
        end

        command = build_select_clause(key, clauses)
        if clauses[:where].kind_of?(Array)
          bind = clauses[:where][1..-1]
        else
          bind = []
        end
        return_value = connection.execute(command, true, self, bind, &blck)

        #distinct
        return_value = return_value.map{|row| row[clauses[:distinct]]}.uniq if clauses[:distinct]

        return_value
      end

      alias find all

      def count(key=nil, clauses={}, &blck)
        key = nil if key == :all
        return_value = nil
        command = build_select_clause(key, clauses.merge({:select_expression=>"count(1)"}))
        if clauses[:where].kind_of?(Array)
          bind = clauses[:where][1..-1]
        else
          bind = []
        end
        rs = connection.execute(command, true, self, bind, &blck)
        if !rs.empty? && rs[0].has_key?('count')
          return_value = rs[0]['count']
        end

        return_value
      end

      def create(clauses={}, options={})
        return false if clauses.empty?

        keys   = clauses.keys.join(', ')
        values = clauses.values.join(', ')

        options_clause = ''

        if !options.empty?
          options_clause = "using " + options.map{|x,y| ' ' + x.to_s + ' ' + y.to_s + ' '}.join(' AND ')
        end

        command = "insert into #{table_name} (#{keys}) values (#{values}) #{options_clause}"
        rs = connection.execute(command, true, self)

        return true
      end

      alias update create

      def delete(key=nil,options={})
        return false unless key

        where_clause = ''
        if key.kind_of?(Array)
          where_clause = " where #{id} in (#{key.join(', ')})"
        elsif key.kind_of?(String)
          where_clause = " where #{id} = #{key}"
        else
          return false
        end

        consistency_clause = ''
        consistency_clause = " using consistency quorum and timestamp #{options[:timestamp]} " if options[:timestamp]

        columns_clause = ''
        columns_clause = options[:columns].join(', ') if options[:columns]
        command = "delete #{columns_clause} from #{table_name} #{consistency_clause} #{where_clause}"
        rs = connection.execute(command, true, self)

        return true
      end

    protected

      def build_select_clause(key=nil, clauses={})
        if clauses[:where].kind_of?(Array)
          where = clauses[:where][0]
        else
          where = clauses[:where]
        end
        where_clause = ''
        if key
          if key.kind_of?(String)
            where_clause = "where #{id} = #{key}"
          elsif key.kind_of?(Array)
            where_clause = "where #{id} in (#{key.join(', ')})"
          end
          if !clauses.empty? && clauses[:where]
            where_clause << ' and ' + where
          end
        elsif !clauses.empty? && clauses[:where]
          where_clause = 'where ' + where
        end

        order_clause = ''
        limit_clause = ''
        if !clauses.empty?
          order_clause = ' order by ' + clauses[:order] if clauses[:order]
          limit_clause = ' limit ' + clauses[:limit].to_s if clauses[:limit]
        end
        command = "select #{clauses[:select_expression]} from #{table_name} #{where_clause} #{order_clause} #{limit_clause};"
        command
      end
    end
  end
end
