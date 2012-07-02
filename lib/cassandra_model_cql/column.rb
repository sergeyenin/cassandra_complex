module CassandraModelCql
  class Column
    attr_reader :name, :value, :timestamp

    def initialize(thrift_column, schema)
      @name  =  CassandraCQL::ColumnFamily.cast(thrift_column.name, schema.names[thrift_column.name])
      @value =  CassandraCQL::ColumnFamily.cast(thrift_column.value, schema.values[thrift_column.name])
      @timestamp = thrift_column.timestamp
    end

    def to_s
      @value.inspect
    end
  end
end
