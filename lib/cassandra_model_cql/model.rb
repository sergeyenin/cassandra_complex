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
  class Model < Table

    class << self

      @attributes = {}

      @options = {}

      @dirty = false

      def attribute(name, type)
        define_method(name) do
          @attributes[name]
        end
        define_method(:"#{name}=") do |value|
          @attributes[name] = value
        end
        self.last_error = nil
      end

      def primary_key
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

    end

  end

end
