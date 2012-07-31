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
  # timelines = Timeline.find('mickey')
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

    attr_accessor :last_error, :last_error_command

    def initialize(*args)
      @attributes = {}
      @options = {:primary_keys => [], :key_space => nil, :dirty => false}
    end

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

    def set_keyspace(keyspace)
      @options[:keyspace] = keyspace
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
    # args[0] : primary key
    # args[1...]: clauses
      self.last_error = nil
    end

    #end

  end

end
