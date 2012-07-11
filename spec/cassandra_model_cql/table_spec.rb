require 'spec_helper'

class Timeline < CassandraModelCql::Table
end

describe "Table" do

  before :all do
    conn = CassandraModelCql::Connection.new('127.0.0.1:9160')
    conn.execute('CREATE KEYSPACE test_spec WITH strategy_class = \'SimpleStrategy\' AND strategy_options:replication_factor = 1;')
    Timeline.set_keyspace('test_spec')
  end

  after :all do
    conn = CassandraModelCql::Connection.new('127.0.0.1:9160')
    conn.execute('DROP KEYSPACE test_spec;')
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

    it 'multiline query' do
      request = <<-eof
        CREATE TABLE timeline (
          user_id varchar,
          tweet_id int,
            author varchar,
            body varchar,
            PRIMARY KEY (user_id, tweet_id));
      eof
      Timeline.execute(request)
      Timeline.last_error.should == nil
    end

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

  context 'create' do
    it 'create record' do
      Timeline.create({'user_id' => "'test_user8'", 'tweet_id' => '8', 'author' => "'test_author8'", 'body' => "'test_body8'"})
      Timeline.last_error.should == nil
      result = Timeline.all('test_user8')
      Timeline.last_error.should == nil
      result.size.should == 1
      result[0]['user_id'].should == 'test_user8'
    end
  end

  context 'update' do
    it 'update record' do
      Timeline.create({'user_id' => "'test_user9'", 'tweet_id' => '9', 'author' => "'test_author9'", 'body' => "'test_body9'"})
      Timeline.last_error.should == nil
      result = Timeline.all('test_user9')
      Timeline.last_error.should == nil
      result.size.should == 1
      result[0]['user_id'].should == 'test_user9'
    end
  end

  context 'delete' do

    it 'single key' do
      Timeline.delete('\'test_user0\'')
      Timeline.last_error.should == nil
      result = Timeline.all('test_user0')
      Timeline.last_error.should == nil
      result.size.should == 0
    end

    it 'multi key' do
      Timeline.delete(['\'test_user1\'','\'test_user2\''])
      Timeline.last_error.should == nil
      result = Timeline.all('test_user1')
      Timeline.last_error.should == nil
      result.size.should == 0
      result = Timeline.all('test_user2')
      Timeline.last_error.should == nil
      result.size.should == 0
    end

    it 'single key with options' do
      Timeline.delete('\'test_user6\'',{:columns => ['author', 'body']})
      Timeline.last_error.should == nil
    end

  end

end

