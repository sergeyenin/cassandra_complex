module CassandraComplex

  class ConfigurationError < Exception
  end

  class MissingConfiguration < ConfigurationError
  end

  # Configuration class to specify basic settings,
  # such as host, default keyspace and logger.
  #
  # Your yaml configuration file should looks like:
  #   host: '127.0.0.1:9160, example.com:9160'
  #   default_keyspace: 'keyspace_production'
  #
  # @!attribute [r] host
  #   @return [String] The host is being connected to
  # @!attribute [r] default_keyspace
  #   @return [String] The keyspace is being used within connection by default
  # @!attribute [rw] logger
  #   @return [String] The logger(kind_of? Logger) is being used by default
  class Configuration

    class << self
      attr_reader :host
      attr_reader :default_keyspace

      attr_accessor :logger

      # Load yaml source
      #
      # @param [IO, String, Hash] something file path, IO, raw YAML string, or a pre-loaded Hash
      # @return [Boolean, Hash] loaded yaml file or false if a RuntimeError occurred while loading
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

        @logger ||= Logger.new('/dev/null')

        return_value
      end

    end
  end
end
