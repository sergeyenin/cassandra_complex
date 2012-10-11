module CassandraComplex
  # A little bit sugared model.
  # @example Using model
  # class Timeline < CassandraComplex::Model
  #
  #   table 'timeline'
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

      @@table       = Hash.new {|hash, key| hash[key] = nil}
      @@table_name  = Hash.new {|hash, key| hash[key] = ''}

      @@attributes  = Hash.new {|hash, key| hash[key] = {}}
      @@primary_key = Hash.new {|hash, key| hash[key] = []}

      # Table executing all cql commands.
      def table_cql
        @@table[self]
      end

      # Table name
      def table_name
        @@table_name[self]
      end

      # All attributes within class.
      def attributes
        @@attributes[self]
      end

      def get_primary_key
        @@primary_key[self]
      end

      def schema
        attr = {}
        attributes.each{|x,y| attr[x] = y[:type]}
        {:table => table_name, :attributes => attr, :primary_key => get_primary_key}
      end

      def primary_key(*attr_names)
        attr_names.each do |attr_name|
          raise WrongModelDefinition, 'Primary key could be choosen just from already introduced attribute.'\
                                                                       unless attributes.has_key?(attr_name.intern)
          @@primary_key[self] << attr_name.intern
        end
      end

      def attribute(attr_name, attr_type)
        attr_name = attr_name.intern
        raise WrongModelDefinition, 'You can`t redefine already introduced attribute.' if self.instance_methods.include?(name)

        attributes[attr_name] = {:type => attr_type}
        define_method(attr_name) do
          @_attributes[attr_name][:value]
        end
        define_method(:"#{attr_name}=") do |value|
          @_attributes[attr_name][:value]  = value
          @_attributes[attr_name][:dirty?] = true
        end
      end

      def table(new_table_name)
        table_name  = new_table_name.to_s.downcase
        @@table[self]   = Class.new(CassandraComplex::Table) do
          set_table_name table_name
        end
        @@table_name[self] = table_name
      end

      def count(key=nil, clauses={}, &blck)
        key = nil if key == :all
        table_cql.count(key, clauses, &blck)
      end

      def all(clauses={}, &blck)
        find(:all, clauses, &blck)
      end

      def find(key=nil, clauses={}, &blck)
        key = nil if key == :all
        return_value = table_cql.find(key, clauses).map do |record|
          new_instance = self.new(record, {:dirty => false})
          blck.call(new_instance) if block_given?
          new_instance
        end
        return_value
      	end

      def delete(key, clauses={}, &blck)
        key = nil if key == :all
        table_cql.delete(key, clauses, &blck)
      end

      def create(hsh={})
        new_model = self.new(hsh)
        new_model.save
        new_model
      end

      def create_table
        attrs = attributes.map{|x,y| "#{x.to_s} #{y[:type].to_s}"}.join(', ')
        p_key = ''
        p_key = " PRIMARY KEY (#{get_primary_key.map{|x| x.to_s}.join(', ')})"
        create_table_command = <<-eos
          CREATE TABLE table_name (
            #{attrs}
            #{p_key}
          );
        eos
        table_cql.execute(create_table_command)
      end

      def drop_table
        drop_table_command = <<-eos
          DROP TABLE table_name;
        eos
        table_cql.execute(drop_table_command)
      end

      def truncate
        table_cql.truncate
      end
    end

    #
    # instance methods
    #

    def dirty?
      return_value = false
      @_attributes.each_value do |attr_value|
        return_value = true if attr_value[:dirty?]
      end
      return_value
    end

    def initialize(hsh = {}, options={})
      @_attributes = Hash.new{|hash, key| hash[key] = {}}
      hsh.each_pair do |key, value|
        if self.class.attributes.has_key?(key.intern)
          @_attributes[key.intern][:value] = value
          @_attributes[key.intern][:dirty?] = options[:dirty].nil? ? true : options[:dirty]
        else
          raise WrongModelInitialization, "Can`t initialize Model with attribute - #{key} ,that are not described in Model definition."
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

    def save
      insert_hash = {}

      @_attributes.keys.each do |key|
        insert_hash[key.to_s] = self.send(key) if self.class.get_primary_key.include?(key) || @_attributes[key.intern][:dirty?]
      end

      self.class.table_cql.create(insert_hash)
      self.dirty = false
    end

    def delete
      delete_hash = {}
      self.class.get_primary_key.each{|pk| delete_hash[pk.to_s]=self.send(pk)}
      self.class.table_cql.delete(delete_hash)
      self.dirty = false
    end

    private

    def dirty=(new_dirty_value, columns = nil)
      columns ||= @_attributes.keys
      @_attributes.each_key do |attr_name|
        @_attributes[attr_name][:dirty?] = new_dirty_value if columns.include?(attr_name)
      end
    end

  end
end
