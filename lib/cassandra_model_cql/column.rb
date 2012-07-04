module CassandraModelCql
  # Cassandra Column
  #
  # @!attribute [r] name
  #   @return [String] Column name
  # @!attribute [r] timestamp
  #   @return [Time] Column timestamp
  # @!attribute [w] dirty
  #   @return [Boolean] dirtyness of Column
  # @!attribute value
  #   @return value of Column
  class Column
    attr_reader   :name, :timestamp
    attr_writer   :dirty
    attr_accessor :value

    # Create instance of Column
    #
    # @param [CassandraCQL::Thrift::Column] thrift_column Original thrift column
    # @param [CassandraCQL::Schema] schema Row schema
    # @return [CassandraModelCql::Column] new instance
    def initialize(thrift_column, schema)
      @dirty = false
      @name  =  CassandraCQL::ColumnFamily.cast(thrift_column.name, schema.names[thrift_column.name])
      @value =  CassandraCQL::ColumnFamily.cast(thrift_column.value, schema.values[thrift_column.name])
      @timestamp = thrift_column.timestamp
    end

    # Return value.to_s
    #
    # @return [String] column.value.to_s
    def to_s
      @value.to_s
    end

    # Return if Column is dirty
    #
    # @return [Boolean] if column is dirty
    def dirty?
      @dirty
    end

  end
end
