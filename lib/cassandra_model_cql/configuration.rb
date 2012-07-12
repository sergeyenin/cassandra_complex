module CassandraModelCql

  class ConfigurationError < Exception
  end

  class MissingConfiguration < ConfigurationError
  end

  # Your yaml configuration file should looks like:
  #
  # production:
  #   hosts: '127.0.0.1, example.com'
  #   port: 9160
  #   keyspace: 'my_keyspace_production'
  class Configuration

    class << self
      attr_accessor :hosts, :port
      attr_accessor :keyspace

      attr_accessr :logger
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

        return_value
      end
    end
  end
end
