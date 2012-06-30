require 'spec_helper'

describe "Connection" do

  context "initialize()" do

    it "CQL3 support" do
      begin
        require 'cassandra-cql/1.1'
      rescue LoadError => ex
        ex.should == nil
      end
      require 'net/telnet'
      begin
        localhost = Net::Telnet::new("Host" => "localhost", "Timeout" => 10, "Port" => 9160)
        localhost.close
      rescue => ex
        ex.should == nil
      end
      begin
        conn = CassandraModelCql::Connection.new('127.0.0.1:9160')
      rescue => ex
        ex.should == nil
      end
    end

    it "array of hosts" do
      require "socket"
      local_ip = UDPSocket.open {|s| s.connect("64.233.187.99", 1); s.addr.last}
      begin
        conn = CassandraModelCql::Connection.new(['127.0.0.1:9160', 'localhost:9160', "#{local_ip}:9160"])
      rescue => ex
        ex.should == nil
      end
    end

  end

  context "query()" do

    before :all do
      @conn = CassandraModelCql::Connection.new('127.0.0.1:9160')
    end

    it "create keyspace" do
      @conn.query("CREATE KEYSPACE spec_test_1 WITH strategy_class = 'org.apache.cassandra.locator.SimpleStrategy' AND strategy_options:replication_factor='1';")
    end

    it "multyline request" do
    end

    it "delete keyspace" do
    end

  end

  context "with_keyspace()" do
    it "does something" do
      # pass
    end
  end

  context "batch_query()" do
    it "does something" do
      # pass
    end
  end

end
