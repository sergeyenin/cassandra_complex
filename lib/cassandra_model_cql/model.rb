module CassandraModelCql
  # ActiveRecord like sugared model
  # @example Using model
  # class Timeline < CassandraModelCql::Model
  #
  #   set_keyspace 'history'
  #
  #   attribute :user_id, 'varchar'
  #   attribute :tweet_id, 'uuid'
  #   attribute :author, 'varchar'
  #   attribute :body, 'varchar'
  #   #composite primary key
  #   primary_key :user_id, :tweet_id
  # end
  #
  # #creating Column Family
  # Timeline.create_table
  #
  # t = Timeline.new(:user_id=>'mickey', :tweet_id=>1715, :author=> 'mouse', :body=>"'Hello!'")
  # t.save
  #
  # timelines = Timeline.all('mickey')
  # t = timelines.first
  # puts t.body
  # => 'Hello!'
  # t.body  = "'Goodbye!'"
  # puts t.dirty?
  # => true
  # t.save
  #
  # t.update(:tweet_id=>1777)
  #
  # #dropping Column Family
  # Timeline.drop_table
  class Model

    @@options = {}

    def initialize(*args)
      @attributes = {}
      @options = {:primary_keys => [], :keyspace => nil, :dirty => false}
      @conn = nil
    end

    class << self

      attr_accessor :last_error, :last_error_command

      def attribute(name, type)
        self.last_error = nil
        self.last_error = 'Attribute name must be a Symbol' if name.class != Symbol
        self.last_error = 'Attribute type must be a String' if type.class != String
        self.last_error = 'Attribute type must be a valid Data Type' if type.size == 0

        @attributes[name]={:value => nil, :type => type, :dirty => true }

        self.class.send(:define_method, name) do
          @attributes[name][:value]
        end
        self.class.send(:define_method, :"#{name}=") do |value|
          @attributes[name][:value] = value
          @attributes[name][:dirty] = true
          @options[:dirty] = true
        end

        self.last_error_command = 'attribute' if self.last_error != nil
      end

      def primary_key(*columns)
        columns.each do |key|
          if @attributes[key]
            @options[:primary_keys].push(key)
          else
            self.last_error = 'Unknown attribute'
            self.last_error_command = 'primary_key'
            @options[:primary_keys]=[]
          end
        end
      end

      def set_host(host)
        @@options[:host] = host
      end

      def set_keyspace(keyspace)
        @@options[:keyspace] = keyspace
      end

      def keyspace
        @options[:keyspace]
      end

      def create_table
        self.last_error = nil
      end

      def drop_table
        self.last_error = nil
      end

      def update(*columns)
      # columns - arrays of pairs: key => value
        self.last_error = nil
      end

      def dirty?
        self.last_error = nil
        @dirty
      end

      def save
        self.last_error = nil
      end

      def delete(clauses = {})
        self.last_error = nil
      end

      def find(*args, &blk)
      # args[0] : primary key
      # args[1...]: clauses
        self.last_error = nil
      end

      def all(*args, &blk)
      #{{{
        self.connect
        return [] if self.last_error
        sql = 'select * from '
        condition = []
        bind = []
        args.each do |arg|
          if arg.class == Hash && arg[:where] && arg[:where].class == Array
            condition.push(arg[:where][0])
            bind.push(arg[:where][1])
          end
        end
        sql += self.name.downcase
        if bind.size > 0
          where = ''
          condition.each do |cond|
            where += ' and ' if where.size > 0
            where += cond
          end
          sql += " where #{where} "
          @result = @connection.conn.execute(sql,bind)
        else
          @result = @connection.conn.execute(sql)
        end
        self.last_error = nil
        self.last_error_command = 'CassandraModelCQL::Model::all'
        self.result2array
      #}}}
      end

      protected

      def connect
      #{{{
        self.last_error = nil
        return @connection if @connection

        host = nil
        if @@options && @@options[:host]
          host = @@options[:host]
        else
          if CassandraModelCql::Configuration.host
            keyspace = CassandraModelCql::Configuration.host
          end
        end
        if host.nil?
          self.last_error = 'Host is nil'
          self.last_error_command = 'CassandraModelCql::Model::connect'
          return nil
        end

        keyspace = nil
        if @@options && @@options[:keyspace]
          keyspace = @@options[:keyspace]
        else
          if CassandraModelCql::Configuration.default_keyspace
            keyspace = CassandraModelCql::Configuration.default_keyspace
          end
        end
        if keyspace.nil?
          self.last_error = 'Keyspaces is nil'
          self.last_error_command = 'CassandraModelCql::Model::connect'
          return nil
        end
        @connection = CassandraModelCql::Connection.new(host, {:keyspace => keyspace})
      #}}}
      end

      def result2array
      #{{{
        result = []
        if @result.class == CassandraCQL::Result
          @result.fetch { |row| result.push(row.to_hash) }
        else
          self.last_error = 'Result is not CassandraCQL::Result'
        end

        result
      #}}}
      end

    end

  end

end
