require 'spec_helper'

class Timeline < CassandraModelCql::Table
end

describe "Table" do

  before :all, :focus=>true do
    conn = CassandraModelCql::Connection.new('127.0.0.1:9160')
    conn.execute('CREATE KEYSPACE spec_test WITH strategy_class = \'SimpleStrategy\' AND strategy_options:replication_factor = 1;')
    CassandraModelCql::Configuration.read({'host'=>'127.0.0.1:9160', 'default_keyspace'=>'spec_test'})
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

  after :all, :focus=>true do
    conn = CassandraModelCql::Connection.new('127.0.0.1:9160')
    conn.execute('DROP TABLE timeline;')
    conn.execute('DROP KEYSPACE spec_test;')
  end

  context 'connection' do
    it 'works' do
      true
    end
  end

  context 'set_keyspace' do
    it 'works' do
      true
    end
  end

  context 'execute' do

    it 'single requests' do
      request = 'INSERT INTO timeline (user_id,tweet_id,author,body) values (\'test_user0\',0,\'test_author0\',\'test_body0\');'
      Timeline.execute(request)
      Timeline.last_error.should == nil
      request = 'INSERT INTO timeline (user_id,tweet_id,author,body) values (\'test_user1\',1,\'test_author1\',\'test_body1\');'
      Timeline.execute(request)
      Timeline.last_error.should == nil
      request = 'INSERT INTO timeline (user_id,tweet_id,author,body) values (\'test_user2\',2,\'test_author2\',\'test_body2\');'
      Timeline.execute(request)
      Timeline.last_error.should == nil
      request = 'INSERT INTO timeline (user_id,tweet_id,author,body) values (\'test_user6\',6,\'test_author6\',\'test_body6\');'
      Timeline.execute(request)
      Timeline.last_error.should == nil
      request = 'INSERT INTO timeline (user_id,tweet_id,author,body) values (\'test_user7\',7,\'test_author7\',\'test_body7\');'
      Timeline.execute(request)
      Timeline.last_error.should == nil
    end

    it 'block processing' do
      count = 0
      Timeline.execute('SELECT * FROM timeline limit 3;') { count += 1 }
      Timeline.last_error.should == nil
      count.should == 3
    end

  end

  context "table_name"  do
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

    it 'without params' do
      result = Timeline.all
      Timeline.last_error == nil
      result.size.should == 5
    end

    it 'with key' do
      result = Timeline.all('test_user0')
      Timeline.last_error.should == nil
      result.size.should == 1
      result[0]['user_id'].should == 'test_user0'
    end

    it 'with not existing key' do
      result = Timeline.all('test_')
      Timeline.last_error.should == nil
      result.size.should == 0
    end

    it 'with key and where clauses' do
      result = Timeline.all('test_user1', { :where => 'tweet_id > 0' })
      Timeline.last_error.should == nil
      result.size.should == 1
    end

    it 'with array key' do
      result = Timeline.all(['test_user1','test_user2'])
      Timeline.last_error.should == nil
      result.size.should == 2
    end

    it 'without key and with where clauses' do
      result = Timeline.all(nil, { :where => 'user_id = \'test_user0\'' })
      Timeline.last_error.should == nil
      result.size.should == 1
      result[0]['user_id'].should == 'test_user0'
    end

    it 'without key and with order clauses' do
      result = Timeline.all(nil, {:where => 'user_id = \'test_user0\'', :order => 'tweet_id' })
      Timeline.last_error.should == nil
      result.size.should == 1
      result[0]['user_id'].should == 'test_user0'
    end

    it 'without key and with limit clauses' do
      result = Timeline.all(nil, { :limit => 3 })
      Timeline.last_error.should == nil
      result.size.should == 3
    end

    it 'select expression' do
      result = Timeline.all(nil, { :select_expression => 'user_id, author' })
      Timeline.last_error.should == nil
      result[0]['user_id'].should != nil
      result[0]['body'].should == nil
    end

    it 'with block processing' do
      id_sum = 0
      Timeline.all { |element| id_sum += element['tweet_id'] }
      Timeline.last_error.should == nil
      id_sum.should == 16
    end

  end

  context 'find' do

    it 'without params' do
      result = Timeline.find
      Timeline.last_error == nil
      result.size.should == 5
    end

    it 'with key' do
      result = Timeline.find('test_user0')
      Timeline.last_error.should == nil
      result.size.should == 1
      result[0]['user_id'].should == 'test_user0'
    end

    it 'with key and where clauses' do
      result = Timeline.find('test_user1', { :where => 'tweet_id > 0' })
      Timeline.last_error.should == nil
      result.size.should == 1
    end

    it 'without key and with where clauses' do
      result = Timeline.find(nil, { :where => 'user_id = \'test_user0\'' })
      Timeline.last_error.should == nil
      result.size.should == 1
      result[0]['user_id'].should == 'test_user0'
    end

    it 'without key and with order clauses' do
      result = Timeline.find(nil, {:where => 'user_id = \'test_user0\'', :order => 'tweet_id' })
      Timeline.last_error.should == nil
      result.size.should == 1
      result[0]['user_id'].should == 'test_user0'
    end

    it 'with block processing' do
      id_sum = 0
      Timeline.find { |element| id_sum += element['tweet_id'] }
      Timeline.last_error.should == nil
      id_sum.should == 16
    end

  end

  context 'count' do

    it 'without params' do
      result = Timeline.count
      Timeline.last_error == nil
      result.size.should == 1
      result[0]['count'].should == 5
    end

    it 'with key' do
      result = Timeline.count('test_user0')
      Timeline.last_error.should == nil
      result.size.should == 1
      result[0]['count'].should == 1
    end

    it 'with not existing key' do
      result = Timeline.count('test_')
      Timeline.last_error.should == nil
      result.size.should == 1
      result[0]['count'].should == 0
    end

    it 'with key and where clauses' do
      result = Timeline.count('test_user1', { :where => 'tweet_id > 0' })
      Timeline.last_error.should == nil
      result.size.should == 1
      result[0]['count'].should == 1
    end

    it 'with array key' do
      result = Timeline.count(['test_user1','test_user2'])
      Timeline.last_error.should == nil
      result.size.should == 1
      result[0]['count'].should == 2
    end

    it 'without key and with where clauses' do
      result = Timeline.count(nil, { :where => 'user_id = \'test_user0\'' })
      Timeline.last_error.should == nil
      result.size.should == 1
      result[0]['count'].should == 1
    end

    it 'without key and with order clauses' do
      result = Timeline.count(nil, {:where => 'user_id = \'test_user0\'', :order => 'tweet_id' })
      Timeline.last_error.should == nil
      result.size.should == 1
      result[0]['count'].should == 1
    end

    it 'without key and with limit clauses' do
      result = Timeline.count(nil, { :limit => 3 })
      Timeline.last_error.should == nil
      result.size.should == 1
      result[0]['count'].should == 3
    end

    it 'with block processing' do
      id_sum = 0
      Timeline.count { |element| id_sum += element['tweet_id'] }
      Timeline.last_error.should == nil
      id_sum.should == 1
    end

  end

  context 'create' do

    it 'create record' do
      Timeline.create({'user_id' => "'test_user8'", 'tweet_id' => '8', 'author' => "'test_author8'", 'body' => "'test_body8'"})
      Timeline.last_error.should == nil
      result = Timeline.all('test_user8')
      Timeline.last_error.should == nil
      result.size.should == 1
      result[0]['user_id'].should == 'test_user8'
    end

    it 'timestamp' do
      Timeline.create({'user_id' => "'test_user10'", 'tweet_id' => '10', 'author' => "'test_author10'", 'body' => "'test_body10'"}, { :timestamp => 2 })
      Timeline.last_error.should == nil
      Timeline.create({'user_id' => "'test_user10'", 'tweet_id' => '10', 'author' => "'test_author11'", 'body' => "'test_body11'"}, { :timestamp => 1 })
      Timeline.last_error.should == nil
      result = Timeline.all('test_user10')
      Timeline.last_error.should == nil
      result.size.should == 1
      result[0]['author'].should == 'test_author10'
    end

    it 'ttl' do
      Timeline.create({'user_id' => "'test_user11'", 'tweet_id' => '11', 'author' => "'test_author11'", 'body' => "'test_body11'"}, { :ttl => 1 })
      Timeline.last_error.should == nil
      result = Timeline.all('test_user11')
      Timeline.last_error.should == nil
      result.size.should == 1
      sleep(2)
      result = Timeline.all('test_user11')
      Timeline.last_error.should == nil
      result.size.should == 0
    end

  end

  context 'update' do

    it 'update record' do
      Timeline.update({'user_id' => "'test_user9'", 'tweet_id' => '9', 'author' => "'test_author9'", 'body' => "'test_body9'"})
      Timeline.last_error.should == nil
      result = Timeline.all('test_user9')
      Timeline.last_error.should == nil
      result.size.should == 1
      result[0]['user_id'].should == 'test_user9'
    end

    it 'timestamp' do
      Timeline.update({'user_id' => "'test_user10'", 'tweet_id' => '10', 'author' => "'test_author10'", 'body' => "'test_body10'"}, { :timestamp => 2 })
      Timeline.last_error.should == nil
      Timeline.update({'user_id' => "'test_user10'", 'tweet_id' => '10', 'author' => "'test_author11'", 'body' => "'test_body11'"}, { :timestamp => 1 })
      Timeline.last_error.should == nil
      result = Timeline.all('test_user10')
      Timeline.last_error.should == nil
      result.size.should == 1
      result[0]['author'].should == 'test_author10'
    end

    it 'ttl' do
      Timeline.update({'user_id' => "'test_user11'", 'tweet_id' => '11', 'author' => "'test_author11'", 'body' => "'test_body11'"}, { :ttl => 1 })
      Timeline.last_error.should == nil
      result = Timeline.all('test_user11')
      Timeline.last_error.should == nil
      result.size.should == 1
      sleep(2)
      result = Timeline.all('test_user11')
      Timeline.last_error.should == nil
      result.size.should == 0
    end

  end

  context 'delete', :focus=>true do
    before (:each) do
      Timeline.create({'user_id' => "'test_user0'", 'tweet_id' => '0', 'author' => "'test_author0'", 'body' => "'test_body0'"})
      Timeline.create({'user_id' => "'test_user1'", 'tweet_id' => '1', 'author' => "'test_author1'", 'body' => "'test_body1'"})
    end

    after (:each) do
     Timeline.delete("'test_user0'")
     Timeline.delete("'test_user1'")
    end

    it 'single key' do
      Timeline.delete("'test_user0'").should == true
      Timeline.all('test_user0').size.should == 0
    end

    it 'array key' do
      Timeline.delete(['\'test_user0\'','\'test_user1\''])
      Timeline.last_error.should == nil
      result = Timeline.all('test_user0')
      Timeline.last_error.should == nil
      result.size.should == 0
      result = Timeline.all('test_user1')
      Timeline.last_error.should == nil
      result.size.should == 0
    end

    it 'single key with options' do
      Timeline.delete('\'test_user0\'',{:columns => ['author', 'body']}).should == true
    end

  end

end

