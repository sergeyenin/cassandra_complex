# Rough monkey-patch to CassandraCQL::Row for access schema
module CassandraCQL
  # CassandraCQL::Row
  #
  # @!attribute [r] schema
  # @return [CassandraCQL::Schema] Row schema
  class Row
    attr_reader :schema
  end
end
