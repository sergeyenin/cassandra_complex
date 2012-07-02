require 'active_support/hash_with_indifferent_access'

#rough monkey-patch to access schema
module CassandraCQL
  class Row
    attr_reader :schema
  end
end

module CassandraModelCql
  class Row < HashWithIndifferentAccess
    def initialize(thrift_row)
      super
      thrift_row.row.columns.each do |thrift_column|
        column_name =  CassandraCQL::ColumnFamily.cast(thrift_column.name, thrift_row.schema.names[thrift_column.name])
        self.merge!({column_name.intern=>Column.new(thrift_column, thrift_row.schema)})
      end
    end
  end
end
