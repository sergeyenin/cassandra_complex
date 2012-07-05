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

    it "create keyspace" do
      conn = CassandraModelCql::Connection.new('127.0.0.1:9160')
      row = conn.query("CREATE KEYSPACE History WITH strategy_class = 'SimpleStrategy' AND strategy_options:replication_factor = 1;")
      row.class.should == CassandraModelCql::RowSet
      begin
        conn = CassandraModelCql::Connection.new('127.0.0.1:9160',{:keyspace => 'History'})
      rescue => ex
        ex.should == nil
      end
    end

    it "multiline request" do
      request = <<eos
CREATE TABLE timeline (
      user_id varchar,
      tweet_id uuid,
           author varchar,
           body varchar,
           PRIMARY KEY (user_id, tweet_id));
eos
      conn = CassandraModelCql::Connection.new('127.0.0.1:9160',{:keyspace => 'History'})
      row = conn.query("request")
      row.class.should == CassandraModelCql::RowSet
    end

    it "multirequest request" do
      request = []
      request[0] = "INSERT INTO timeline (user_id,tweet_id,author,body) values ('test0','test0','test0','test0');"
      request[1] = "INSERT INTO timeline (user_id,tweet_id,author,body) values ('test1','test1','test1','test1');"
      request[1] = "INSERT INTO timeline (user_id,tweet_id,author,body) values ('test2','test2','test2','test2');"
      conn = CassandraModelCql::Connection.new('127.0.0.1:9160',{:keyspace => 'History'})
      row = conn.query("request",true)
      row.class.should == CassandraModelCql::RowSet
    end

    it "delete keyspace" do
      request = "DROP KEYSPACE History"
      conn = CassandraModelCql::Connection.new('127.0.0.1:9160')
      row = conn.query("request")
      row.class.should == CassandraModelCql::RowSet
    end

  end

  context "with_keyspace()" do
    it "works" do
      conn = CassandraModelCql::Connection.new('127.0.0.1:9160')
      row = conn.query("CREATE KEYSPACE History WITH strategy_class = 'SimpleStrategy' AND strategy_options:replication_factor = 1;")
      row = conn.with_keyspace('History') do
        row = conn.query("CREATE KEYSPACE History WITH strategy_class = 'SimpleStrategy' AND strategy_options:replication_factor = 1;")
        request = <<eos
CREATE TABLE timeline (
      user_id varchar,
      tweet_id uuid,
           author varchar,
           body varchar,
           PRIMARY KEY (user_id, tweet_id));
eos
        row = conn.query("request")
        row.class.should == CassandraModelCql::RowSet
      end
    end
  end

  context "batch_query()" do
    it "works" do
      conn = CassandraModelCql::Connection.new('127.0.0.1:9160',{:keyspace => 'History'})
      request = <<eos
INSERT INTO timeline (user_id,tweet_id,author,body) values ('test0','test0','test0','test_body');
INSERT INTO timeline (user_id,tweet_id,author,body) values ('test1','test1','test1','test_body');
INSERT INTO timeline (user_id,tweet_id,author,body) values ('test2','test2','test2','test_body');
eos
      row = conn.batch_query(request)
      row.class.should == CassandraModelCql::RowSet
      row = conn.query("SELECT * FROM timeline WHERE body = 'test_body'")
      row.class.should == CassandraModelCql::RowSet
      #row.size.should == 3
    end
  end

end
