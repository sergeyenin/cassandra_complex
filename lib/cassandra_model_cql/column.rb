module CassandraModelCql
  class Column
    attr_reader :name, :timestamp
    attr_writer :value, :dirty
    def initialize(thrift_column, schema)
      @dirty = false

      @name  =  CassandraCQL::ColumnFamily.cast(thrift_column.name, schema.names[thrift_column.name])
      @value =  CassandraCQL::ColumnFamily.cast(thrift_column.value, schema.values[thrift_column.name])
      @timestamp = thrift_column.timestamp
    end

    def to_s
      @value.to_s
    end

    def dirty?
      @dirty
    end

  end
end
