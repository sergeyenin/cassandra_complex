module CassandraComplex
  # ActiveRecord like sugared model
  # @example Using model
  # class Timeline < CassandraComplex::Model
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
  # t.save!
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
  class ModelError < Exception
  end

  class WrongModelDefinition < ModelError
  end

  class WrongModelInitialization < ModelError
  end

  class Model
    #class` methods
    class << self
      attr_accessor :table_name

      @@table       = nil
      @@table_name  = ''

      @@attributes  = {}
      @@primary_key = []

      def schema
        attr = {}
        @@attributes.map{|x,y| attr[x] = y[:type]}
        {:table => @@table_name, :attributes => attr, :primary_key => @@primary_key}
      end

      def primary_key
        @@primary_key
      end

      def primary_key(*attr_names)
        attr_names.each do |attr_name|
          raise WrongModelDefinition, 'Primary key could be choosen just from already introduced attribute.' unless @@attributes.has_key?(attr_name.intern)
          @@primary_key << attr_name.intern
        end
      end

      def attributes
        @@attributes
      end

      def attribute(attr_name, attr_type)
        attr_name = attr_name.intern
        raise WrongModelDefinition, 'You can`t redefine already introduced attribute.' if self.instance_methods.include?(name)

        @@attributes[attr_name] = {:type => attr_type}
        define_method(attr_name) do
          @@attributes[attr_name][:value]
        end
        define_method(:"#{attr_name}=") do |value|
          @@attributes[attr_name][:value]  = value
          @@attributes[attr_name][:dirty?] = true
        end
      end

      def table(table_name)
        @@table_name = table_name.to_s.downcase
        @@table      = Class.new(CassandraComplex::Table) do
          set_table_name @@table_name
        end
      end

      def count(key=nil, clauses={}, &blck)
        key = nil if key == :all
        return_value = 0

        return_value = @@table.count(key, clauses, &blck)

        return_value
      end

      def all(clauses={}, &blck)
        find(:all, clauses, &blck)
      end

      def find(key=nil, clauses={}, &blck)
        return_value = []
        key = nil if key == :all
        @@table.find(key, clauses).each do |record|
          new_instance = self.new(record)
          return_value << new_instance
          blck.call(new_instance) if block_given?
        end
        return_value
      	end

      def delete(key, hsh={}, &blck)
        key = nil if key == :all
        @@table.delete(key, hsh, &blck)
      end

      def create!(hsh={})
        new_model = self.new(hsh)
        new_model.save!
        new_model
      end

      def create_table!
        attr = @@attributes.map{|x,y| "#{x.to_s} #{y[:type].to_s}"}.join(', ')
        p_key = ''
        p_key = " PRIMARY KEY (#{@@primary_key.map{|x| x.to_s}.join(', ')})"
        create_table_command = <<-eos
          CREATE TABLE @@table_name (
            #{attr}
            #{p_key}
          );
        eos
        @@table.execute(create_table_command)
      end

      def truncate
        @@table.truncate
      end
    end

    #
    #instance
    #

    def dirty?
      return_value = false
      @@attributes.each_value |attr_value|
        return_value = true if attr_value[:dirty?]
      end
      return_value
    end

    #instance` methods
    def initialize(hsh = {})
      hsh.each_pair do |key, value|
        if @@attributes.has_key?(key)
          raise WrongModelInitialization, "Can`t initialize Model with attribute - #{key} ,that are not described in Model definition."
        else
          @@attributes[key.intern][:value] = value
          @@attributes[key.intern][:dirty?] = true
        end
      end

    end

    def ==(other)
      return false unless other.kind_of?(self.class)
      return false unless self.class.attributes.keys.sort == other.class.attributes.keys.sort

      self.class.attributes.keys.each do |attr|
        return false unless self.send(attr) == other.send(attr)
      end

      return true
    end

    #todo: save just columns which are neccessary
    def save!
      insert_hash = {}

      @@attributes.keys.each do |key|
        insert_hash[key.to_s] = self.send(key)
      end

      @@table.create(insert_hash)
      self.dirty? = false, @@attributes.select{|attr_name, attr_value| attr_value[:dirty?]}.keys
    end

    def delete
      delete_hash = {}
      @@primary_key.map{|pk| delete_hash[pk.to_s]=self.send(pk)}
      @@table.delete(delete_hash)
      self.dirty? = false
    end

    private

    def dirty?=(new_dirty_value, columns = nil)
      columns ||= @@attributes.keys
      @@attributes.each_key do |attr_name|
        @@attributes[attr_name][:dirty?] = new_dirty_value
      end
    end

  end
end
