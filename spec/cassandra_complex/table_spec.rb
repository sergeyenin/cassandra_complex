require 'spec_helper'

class Timeline < CassandraComplex::Table
end

describe "Table" do

  before :all do
    conn = CassandraComplex::Connection.new('127.0.0.1:9160')
    conn.execute('CREATE KEYSPACE cassandra_complex_test WITH strategy_class = \'SimpleStrategy\' AND strategy_options:replication_factor = 1;')
    CassandraComplex::Configuration.read({'host'=>'127.0.0.1:9160', 'default_keyspace'=>'cassandra_complex_test'})
    create_table_command = <<-eos
        CREATE TABLE timeline (
          user_id varchar,
          tweet_id int,
            author varchar,
            body varchar,
            PRIMARY KEY (user_id, tweet_id));
    eos
    Timeline.execute(create_table_command)
  end

  after :all do
    conn = CassandraComplex::Connection.new('127.0.0.1:9160')
    conn.execute('DROP TABLE timeline;')
    conn.execute('DROP KEYSPACE cassandra_complex;')
  end

  context 'execute' do
    before (:each) do
      Timeline.truncate
    end

    after (:all) do
      Timeline.truncate
    end

    it 'single requests' do
      request = 'INSERT INTO timeline (user_id,tweet_id,author,body) values (\'test_user0\',0,\'test_author0\',\'test_body0\');'
      Timeline.execute(request)
      request = 'INSERT INTO timeline (user_id,tweet_id,author,body) values (\'test_user1\',1,\'test_author1\',\'test_body1\');'
      Timeline.execute(request)
      request = 'INSERT INTO timeline (user_id,tweet_id,author,body) values (\'test_user2\',2,\'test_author2\',\'test_body2\');'
      Timeline.execute(request)
      request = 'INSERT INTO timeline (user_id,tweet_id,author,body) values (\'test_user3\',3,\'test_author3\',\'test_body3\');'
      Timeline.execute(request)
      request = 'INSERT INTO timeline (user_id,tweet_id,author,body) values (\'test_user4\',4,\'test_author4\',\'test_body4\');'
      Timeline.execute(request)
      Timeline.all.size.should == 5
    end

    it 'block processing' do
      request = 'INSERT INTO timeline (user_id,tweet_id,author,body) values (\'test_user0\',0,\'test_author0\',\'test_body0\');'
      Timeline.execute(request)
      request = 'INSERT INTO timeline (user_id,tweet_id,author,body) values (\'test_user1\',1,\'test_author1\',\'test_body1\');'
      Timeline.execute(request)
      request = 'INSERT INTO timeline (user_id,tweet_id,author,body) values (\'test_user2\',2,\'test_author2\',\'test_body2\');'

      count = 1
      Timeline.execute('SELECT * FROM timeline limit 3;') { count += 1 }
      count.should == 3
    end

  end

  context "table_name" do
    it "returns correct table name" do
      Timeline.table_name.should == 'timeline'
    end
  end

  context "id" do
    it "returns correct id" do
      Timeline.id.should == 'user_id'
    end
  end

  context 'all' do

    before(:each) do
      Timeline.create({'user_id' => "'test_user0'", 'tweet_id' => '16', 'author' => "'test_author0'", 'body' => "'test_body0'"})
    end

    after(:each) do
      Timeline.truncate
    end

    it 'without params' do
      result = Timeline.all
      result.size.should == 1
    end

    it 'with key' do
      result = Timeline.all("'test_user0'")
      result.size.should == 1
      result[0]['user_id'].should == 'test_user0'
    end

    it 'with not existing key' do
      result = Timeline.all("'test_'")
      result.size.should == 0
    end

    it 'with key and where clauses' do
      result = Timeline.all("'test_user0'", { :where => 'tweet_id = 16' })
      result.size.should == 1
    end

    it 'with array key' do
      Timeline.create({'user_id' => "'test_user1'", 'tweet_id' => '1', 'author' => "'test_author1'", 'body' => "'test_body1'"})
      result = Timeline.all(["'test_user0'","'test_user1'"])
      result.size.should == 2
    end

    it 'without key and with where clauses' do
      result = Timeline.all(nil, { :where => 'user_id = \'test_user0\'' })
      result.size.should == 1
      result[0]['user_id'].should == 'test_user0'
    end

    it 'without key and with order clauses' do
      result = Timeline.all(nil, {:where => 'user_id = \'test_user0\'', :order => 'tweet_id' })
      result.size.should == 1
      result[0]['user_id'].should == 'test_user0'
    end

    it 'without key and with limit clauses' do
      Timeline.create({'user_id' => "'test_user1'", 'tweet_id' => '1', 'author' => "'test_author1'", 'body' => "'test_body1'"})
      result = Timeline.all(nil, { :limit => 1 })
      result.size.should == 1
    end

    it 'select expression' do
      result = Timeline.all(nil, { :select_expression => 'user_id, author' })
      result[0]['user_id'].should_not == nil
      result[0]['body'].should == nil
    end

    it 'with block processing' do
      id_sum = 0
      Timeline.all { |element| id_sum += element['tweet_id'] }
      id_sum.should == 16
    end

    it 'one binding key' do
      result = Timeline.all(nil, { :where => ['user_id = ?', 'test_user0'] })
      result.size.should == 1
      result[0]['user_id'].should == 'test_user0'
    end

    it 'two binding keys' do
      result = Timeline.all(nil, { :where => ['user_id = ? and tweet_id = ?', 'test_user0', 16] })
      result.size.should == 1
      result[0]['user_id'].should == 'test_user0'
    end

    it 'few binding keys' do
      result = Timeline.all(nil, { :where => ['user_id = ? and tweet_id > ? and tweet_id < ?', 'test_user0', 10, 20] })
      result.size.should == 1
      result[0]['user_id'].should == 'test_user0'
    end

  end

  context 'count' do

    before(:each) do
      Timeline.create({'user_id' => "'test_user0'", 'tweet_id' => '16', 'author' => "'test_author0'", 'body' => "'test_body0'"})
    end

    after(:each) do
      Timeline.truncate
    end

    it 'without params' do
      count = Timeline.count
      count.should == 1
    end

    it 'with key' do
      count = Timeline.count("'test_user0'")
      count.should == 1
    end

    it 'with not existing key' do
      count = Timeline.count("'test_'")
      count.should == 0
    end

    it 'with key and where clauses' do
      count = Timeline.count("'test_user0'", { :where => 'tweet_id >= 10' })
      count.should == 1
    end

    it 'with array key' do
      Timeline.create({'user_id' => "'test_user1'", 'tweet_id' => '16', 'author' => "'test_author1'", 'body' => "'test_body1'"})
      count = Timeline.count(["'test_user0'","'test_user1'"])
      count.should == 2
    end

    it 'without key and with where clauses' do
      count = Timeline.count(nil, { :where => 'user_id = \'test_user0\'' })
      count.should == 1
    end

    it 'without key and with order clauses' do
      count = Timeline.count(nil, {:where => 'user_id = \'test_user0\'', :order => 'tweet_id' })
      count.should == 1
    end

    it 'without key and with limit clauses' do
      Timeline.create({'user_id' => "'test_user1'", 'tweet_id' => '16', 'author' => "'test_author1'", 'body' => "'test_body1'"})
      count = Timeline.count(nil, { :limit => 1 })
      count.should == 1
    end

    it 'with block processing' do
      Timeline.create({'user_id' => "'test_user1'", 'tweet_id' => '16', 'author' => "'test_author1'", 'body' => "'test_body1'"})
      id_sum = 0
      Timeline.count { |element| id_sum += element['count'] }
      id_sum.should == 2
    end

  end

  context 'create' do

    before (:each) do
      Timeline.truncate
    end

    after (:all) do
      Timeline.truncate
    end

    it 'create record' do
      Timeline.create({'user_id' => "'test_user0'", 'tweet_id' => '0', 'author' => "'test_author0'", 'body' => "'test_body0'"})
      timelines = Timeline.all("'test_user0'")
      timelines.size.should == 1
      timelines[0]['user_id'].should == 'test_user0'
    end

    it 'timestamp' do
      Timeline.create({'user_id' => "'test_user0'", 'tweet_id' => '0', 'author' => "'test_author0'", 'body' => "'test_body0'"}, { :timestamp => 2 })
      Timeline.create({'user_id' => "'test_user0'", 'tweet_id' => '0', 'author' => "'test_author1'", 'body' => "'test_body1'"}, { :timestamp => 1 })
      Timeline.all.size.should == 1
      result = Timeline.all("'test_user0'")
      result.size.should == 1
      result[0]['author'].should == 'test_author0'
    end

    it 'ttl' do
      Timeline.create({'user_id' => "'test_user1'", 'tweet_id' => '1', 'author' => "'test_author1'", 'body' => "'test_body1'"}, { :ttl => 1 })
      result = Timeline.all("'test_user1'")
      result.size.should == 1
      sleep(2)
      result = Timeline.all("'test_user1'")
      result.size.should == 0
    end

  end

  context 'update' do

    before (:each) do
      Timeline.truncate
    end

    after (:all) do
      Timeline.truncate
    end

    it 'update record' do
      Timeline.create({'user_id' => "'test_user0'", 'tweet_id' => '0', 'author' => "'test_author0'", 'body' => "'test_body0'"})
      Timeline.update({'user_id' => "'test_user0'", 'tweet_id' => '0', 'author' => "'test_author0'", 'body' => "'test_body1'"})
      result = Timeline.all("'test_user0'")
      result.size.should == 1
      result[0]['body'].should == 'test_body1'
    end

    it 'timestamp' do
      Timeline.create({'user_id' => "'test_user0'", 'tweet_id' => '0', 'author' => "'test_author0'", 'body' => "'test_body0'"}, { :timestamp => 2 })
      Timeline.update({'user_id' => "'test_user0'", 'tweet_id' => '0', 'author' => "'test_author0'", 'body' => "'test_body1'"}, { :timestamp => 1 })
      result = Timeline.all("'test_user0'")
      result.size.should == 1
      result[0]['author'].should == 'test_author0'
    end

    it 'ttl' do
      Timeline.update({'user_id' => "'test_user0'", 'tweet_id' => '0', 'author' => "'test_author0'", 'body' => "'test_body0'"}, { :ttl => 1 })
      result = Timeline.all("'test_user0'")
      result.size.should == 1
      sleep(2)
      result = Timeline.all("'test_user0'")
      result.size.should == 0
    end

  end

  context 'delete' do
    before (:each) do
      Timeline.truncate
      Timeline.create({'user_id' => "'test_user0'", 'tweet_id' => '0', 'author' => "'test_author0'", 'body' => "'test_body0'"})
      Timeline.create({'user_id' => "'test_user1'", 'tweet_id' => '1', 'author' => "'test_author1'", 'body' => "'test_body1'"})
    end

    after (:each) do
     Timeline.delete("'test_user0'")
     Timeline.delete("'test_user1'")
    end

    it 'single key' do
      Timeline.delete("'test_user0'").should == true
      Timeline.all("'test_user0'").size.should == 0
    end

    it 'array key' do
      Timeline.delete(["'test_user0'","'test_user1'"]).should == true
      Timeline.all(["'test_user0'", "'test_user1'"]).size.should == 0
    end

    it 'single key with options' do
      Timeline.delete("'test_user0' and tweet_id=0", {:columns => ['author', 'body']}).should == true
      Timeline.all.size.should == 1
    end

    it 'supports timestamp' do
       Timeline.delete("'test_user0' and tweet_id=0", { :timestamp=> (Time.now.to_i + 10) }).should == true
       Timeline.all.size.should == 2
    end

  end

end

