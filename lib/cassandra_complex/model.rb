module CassandraComplex
  # A little bit sugared model.
  #
  # @!attribute [rw] table_name
  #   @return [String] Table name
  #
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
      @@secondary_index = Hash.new{|hash, key| hash[key] = {}}

      # Returns table executing all cql commands
      #
      # @return [Table] CassandraComplex::Table
      def table_cql
        @@table[self]
      end

      # Returns table name
      #
      # @return [String] Name of the table
      def table_name
        @@table_name[self]
      end

      # Returns all attributes within class
      #
      # @return [Hash] attributes of current Model
      def attributes
        @@attributes[self]
      end

      # Returns primary key for current Model
      #
      # @return [Array<String>] primary key for current Model
      def get_primary_key
        @@primary_key[self]
      end

      # Returns schema for current Model
      #
      # @return [Hash] schema for current model
      def schema
        attr = {}
        attributes.each{|x,y| attr[x] = y[:type]}
        return_value = {:table => table_name, :attributes => attr, :primary_key => get_primary_key}
        return_value.merge!(:secondary_index => @@secondary_index[self]) unless @@secondary_index[self].empty?
        return_value
      end

      # Set primary key(s) for current Model
      #
      # @param [Array<Symbol>] attr_names Primary key(s)
      def primary_key(*attr_names)
        attr_names.each do |attr_name|
          raise WrongModelDefinition, 'Primary key could be choosen just from already introduced attribute.'\
                                                                       unless attributes.has_key?(attr_name.intern)
          @@primary_key[self] << attr_name.intern
        end
      end

      # Add secondary index for current Model
      #
      # @param [String] secondary_idx New secondary index
      def secondary_index(secondary_idx, idx_name='')
        raise WrongModelDefinition, 'Secondary index could be choosen just from already introduced attribute.'\
                                                                       unless attributes.has_key?(secondary_idx.intern)
        idx_name = self.to_s.downcase + '_' + secondary_idx.to_s + '_idx' if idx_name.empty?
        @@secondary_index[self][idx_name] = secondary_idx.intern
      end

      # Introduce attribute for the Model.
      # Valid attribute`s types: 'blog', 'ascii', 'text'/'varchar', 'varint', 'int', 'bigint', 'uuid', 'timestamp', 'boolean',
      # 'float', 'double', 'decimal', 'counter'.
      #
      # @param [Symbol] attr_name Attribute`s name
      # @param [String] attr_type Attribute`s type
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

      # Set table name for current Model
      #
      # @param [String] new_table_name
      def table(new_table_name)
        table_name  = new_table_name.to_s.downcase
        @@table[self]   = Class.new(CassandraComplex::Table) do
          set_table_name table_name
        end
        @@table_name[self] = table_name
      end

      # Return count of result set for given primary key
      #
      # @param [String, Array<String>, Hash] key
      # @param [Hash] clauses select clauses
      # @option clauses [String, Array<String>] where where clause
      # @option clauses [String] order order clause
      # @option clauses [String] limit limit clause
      # @yieldparam [Proc] blck custom code
      # @return [Array<Hash>] array of hashes
      def count(key=nil, clauses={}, &blck)
        key = nil if key == :all
        table_cql.count(key, clauses, &blck)
      end

      # Return 'all' result set
      #
      # @param [Hash] clauses select clauses
      # @option clauses [String, Array<String>] where where clause
      # @option clauses [String] order order clause
      # @option clauses [String] limit limit clause
      # @yieldparam [Proc] blck custom code
      # @return [Array<Hash>] array of hashes
      def all(clauses={}, &blck)
        find(:all, clauses, &blck)
      end

      # Return result set for given primary key
      #
      # @param [String, Array<String>, Hash] key
      # @param [Hash] clauses select clauses
      # @option clauses [String, Array<String>] where where clause
      # @option clauses [String] order order clause
      # @option clauses [String] limit limit clause
      # @yieldparam [Proc] blck custom code
      # @return [Array<Hash>] array of hashes
      def find(key=nil, clauses={}, &blck)
        key = nil if key == :all
        return_value = table_cql.find(key, clauses).map do |record|
          new_instance = self.new(record, {:dirty => false})
          blck.call(new_instance) if block_given?
          new_instance
        end
        return_value
      end

      # Delete record(s)
      #
      # @param [String, Array<String>, Hash] key
      # @param [Hash] clauses
      # @option clauses [String] timestamp timestamp of operation
      # @option clauses [Array<String>] columns columns which should be deleted
      # @option clauses [Array, String] where where options for delete operation
      # @return [Boolean] always true
      def delete(key, clauses={}, &blck)
        key = nil if key == :all
        table_cql.delete(key, clauses, &blck)
      end

      # Create record from hash
      #
      # @param [Hash] hsh attributes for new record
      # @return [Model] created model
      def create(hsh={})
        new_model = self.new(hsh)
        new_model.save
        new_model
      end

      # Create table for model within Cassandra
      def create_table
        attrs = attributes.map{|x,y| "#{x.to_s} #{y[:type].to_s}"}.join(', ')
        p_key = ''
        p_key = ", PRIMARY KEY (#{get_primary_key.map{|x| x.to_s}.join(', ')})"
        s_idxs = []
        s_idxs = @@secondary_index[self].collect{|k,v| "CREATE INDEX #{k} ON #{self.to_s.downcase} (#{v.to_s});"}
        create_table_command = <<-eos
          CREATE TABLE #{table_name} (
            #{attrs}
            #{p_key}
          );
        eos
        table_cql.execute(create_table_command)

        s_idxs.each do |s_idx|
          table_cql.execute(s_idx)
        end
      end

      # Drop table for model within Cassandra
      def drop_table
        # no need to drop secondary indexes, they will be deleted on drop table automatically
        drop_table_command = <<-eos
          DROP TABLE table_name;
        eos
        table_cql.execute(drop_table_command)
      end

      # Truncate table within Cassandra
      def truncate
        table_cql.truncate
      end
    end

    # Returns if model is dirty(some field were changed but model still not saved)
    #
    # @return [Boolean] is model dirty
    def dirty?
      return_value = false
      @_attributes.each_value do |attr_value|
        return_value = true if attr_value[:dirty?]
      end
      return_value
    end

    # Initialize model with hash due to options
    #
    # @param [Hash] hsh initialization attributes
    # @param [Hash] options initalization options
    # @option options [Bolean] :dirty dirtiness of initialized model
    # @raise [WrongModelInitialization] raised if passed previously not described attribute
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

    # Comparator for current and other model
    #
    # @param [Model] other model to compare with
    # @return [Boolean] true if attrbitues of models are equal and each attribute of other model equal to proper attribute of current model, false otherwise
    def ==(other)
      return false unless other.kind_of?(self.class)
      return false unless self.class.attributes.keys.sort == other.class.attributes.keys.sort

      self.class.attributes.keys.each do |attr|
        return false unless self.send(attr) == other.send(attr)
      end

      return true
    end

    # Save the Model within Cassandra
    def save
      insert_hash = {}

      @_attributes.keys.each do |key|
        insert_hash[key.to_s] = self.send(key) if self.class.get_primary_key.include?(key) || @_attributes[key.intern][:dirty?]
      end

      self.class.table_cql.create(insert_hash)
      self.dirty = false
    end

    # Delete the Model within Cassandra
    def delete
      delete_hash = {}
      self.class.get_primary_key.each{|pk| delete_hash[pk.to_s]=self.send(pk)}
      self.class.table_cql.delete(delete_hash)
      self.dirty = false
    end

    private

    # Set dirtiness for the whole Model
    def dirty=(new_dirty_value, columns = nil)
      columns ||= @_attributes.keys
      @_attributes.each_key do |attr_name|
        @_attributes[attr_name][:dirty?] = new_dirty_value if columns.include?(attr_name)
      end
    end

  end
end
