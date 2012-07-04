require 'active_support/hash_with_indifferent_access'

# Rough monkey-patch to CassandraCQL::Row for access schema
module CassandraCQL
  # CassandraCQL::Row
  #
  # @!attribute [r] schema
  #   @return [CassandraCQL::Schema] Row schema
  class Row
    attr_reader :schema
  end
end

module CassandraModelCql

  # CassandraModelCql::Row is hash like accessable row
  class Row < HashWithIndifferentAccess

    # Create instance of Row
    #
    # @param [Hash] hsh
    # @option hsh [CassandraCQL::Row] :thrift_row Thrift row
    # @option hsh [CassandraModelCql::Table] :table Table which consist that row
    # @return [CassandraModelCql::Row] new instance
    def initialize(hsh = {})
      thrift_row = hsh.delete(:thrift_row)
      @table = hsh.delete(:table)
      super
      thrift_row.row.columns.each do |thrift_column|
        column_name =  CassandraCQL::ColumnFamily.cast(thrift_column.name, thrift_row.schema.names[thrift_column.name])
        self.merge!({column_name.intern=>Column.new(thrift_column, thrift_row.schema)})
      end
    end

    # Rewriten []= assigner, which mark accessed value as dirty
    #
    # @param [String, Symbol] key The key of hash map
    # @param val The value of hash map
    def []=(key, val)
      self[key].value = val
      self[key].dirty = true
    end

    # @todo dont dorget to implement validation and callbacks
    # Save the row
    #
    # @return [Boolean] if the row was saved successful
    def save()
      return false unless @table

      table_name  = @table.to_s.downcase
      primary_key = @table.primary_key
      set_clause_array = []
      self.each_pair do |key, value|
        set_clause_array << "#{key.to_s}='#{value.to_s}'" if value.dirty?
      end

      return true if set_clause_array.empty?

      update = "UPDATE #{@table.table_name} SET #{set_clause_array.join(',')} WHERE #{@table.primary_key} = #{self[@table.primary_key].to_s}";
      @table.connection.query(update)

      return true
    end

  end
end
