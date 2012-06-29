module CassandraModelCql
  class RowSet

    include Enumerable

    attr_reader :rows

    def initialize(hash={})
      rows = hash.dup
    end

    def add(hash)
      rows.merge(hash)
    end

    def each
      raise NotImplementedError
    end

  end
end
