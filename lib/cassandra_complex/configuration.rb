module CassandraComplex

  class ConfigurationError < Exception
  end

  class MissingConfiguration < ConfigurationError
  end

  # Your yaml configuration file should looks like:
  #
  # production:
  #   host: '127.0.0.1:9160', example.com:9160'
  #   default_keyspace: 'my_keyspace_production'
  class Configuration

    class << self
      attr_reader :host
      attr_reader :default_keyspace

      # Load yaml source
      #
      # === Parameters
      # something(IO|String|Hash):: File path, IO, raw YAML string, or a pre-loaded Hash
      #
      # === Returns
      # (Boolean|Hash):: Loaded yaml file or false if a RuntimeError occurred while loading
      def read(something)
        return_value = false

        begin
          if something.kind_of?(Hash)
            return_value = something
          elsif File.exists?(something)
            return_value = YAML.load_file(something)
          else
            return_value = YAML.load(something)
          end
          raise ConfigurationError unless return_value.kind_of?(Hash)
        rescue
          return_value = false
        end
        @host = return_value['host']
        @default_keyspace = return_value['default_keyspace']
        return_value
      end

    end
  end
end
