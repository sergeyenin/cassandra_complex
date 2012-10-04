require 'spec_helper'

class TimelineTable < CassandraComplex::Table
  set_table_name 'timeline'
end

CassandraComplex::Configuration.logger = Logger.new(STDOUT)

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
    TimelineTable.execute(create_table_command)
  end

  after :all do
    conn = CassandraComplex::Connection.new('127.0.0.1:9160')
    conn.execute('DROP TABLE timeline;')
    conn.execute('DROP KEYSPACE cassandra_complex;')
  end

  context 'execute' do
    before (:each) do
      TimelineTable.truncate
    end

    after (:all) do
      TimelineTable.truncate
    end

    it 'single requests' do
      request = 'INSERT INTO timeline (user_id,tweet_id,author,body) values (\'test_user0\',0,\'test_author0\',\'test_body0\');'
      TimelineTable.execute(request)
      request = 'INSERT INTO timeline (user_id,tweet_id,author,body) values (\'test_user1\',1,\'test_author1\',\'test_body1\');'
      TimelineTable.execute(request)
      request = 'INSERT INTO timeline (user_id,tweet_id,author,body) values (\'test_user2\',2,\'test_author2\',\'test_body2\');'
      TimelineTable.execute(request)
      request = 'INSERT INTO timeline (user_id,tweet_id,author,body) values (\'test_user3\',3,\'test_author3\',\'test_body3\');'
      TimelineTable.execute(request)
      request = 'INSERT INTO timeline (user_id,tweet_id,author,body) values (\'test_user4\',4,\'test_author4\',\'test_body4\');'
      TimelineTable.execute(request)
      TimelineTable.all.size.should == 5
    end

    it 'block processing' do
      request = 'INSERT INTO timeline (user_id,tweet_id,author,body) values (\'test_user0\',0,\'test_author0\',\'test_body0\');'
      TimelineTable.execute(request)
      request = 'INSERT INTO timeline (user_id,tweet_id,author,body) values (\'test_user1\',1,\'test_author1\',\'test_body1\');'
      TimelineTable.execute(request)
      request = 'INSERT INTO timeline (user_id,tweet_id,author,body) values (\'test_user2\',2,\'test_author2\',\'test_body2\');'

      count = 1
      TimelineTable.execute('SELECT * FROM timeline limit 3;') { count += 1 }
      count.should == 3
    end

  end

  context 'table_name' do
    it 'returns correct table name' do
      TimelineTable.table_name.should == 'timeline'
    end

    it 'set proper table_name' do
      module SomeModule
        class TimelineTable < CassandraComplex::Table
          set_table_name 'timeline'
        end
      end

      SomeModule::TimelineTable.table_name.should == 'timeline'
    end

  end

  context 'id' do
    it 'returns correct id' do
      TimelineTable.id.should == 'user_id'
    end
  end

  context 'all' do

    before(:each) do
      TimelineTable.create({'user_id' => 'test_user0', 'tweet_id' => 16, 'author' => 'test_author0', 'body' => 'test_body0'})
    end

    after(:each) do
      TimelineTable.truncate
    end

    it 'without params' do
      result = TimelineTable.all
      result.size.should == 1
    end

    it 'with key' do
      result = TimelineTable.all('test_user0')
      result.size.should == 1
      result[0]['user_id'].should == 'test_user0'
    end

    it 'with not existing key' do
      result = TimelineTable.all('test_')
      result.size.should == 0
    end

    it 'with key and where clauses' do
      result = TimelineTable.all('test_user0', { :where => 'tweet_id = 16' })
      result.size.should == 1
    end

    it 'with array key' do
      TimelineTable.create({'user_id' => 'test_user1', 'tweet_id' => 1, 'author' => 'test_author1', 'body' => 'test_body1'})
      result = TimelineTable.all(['test_user0', 'test_user1'])
      result.size.should == 2
    end

    it 'without key and with where clauses' do
      result = TimelineTable.all(:all, { :where => 'user_id = \'test_user0\'' })
      result.size.should == 1
      result[0]['user_id'].should == 'test_user0'
    end

    it 'without key and with order clauses' do
      result = TimelineTable.all(:all, {:where => 'user_id = \'test_user0\'', :order => 'tweet_id' })
      result.size.should == 1
      result[0]['user_id'].should == 'test_user0'
    end

    it 'without key and with limit clauses' do
      TimelineTable.create({'user_id' => 'test_user1', 'tweet_id' => 1, 'author' => 'test_author1', 'body' => 'test_body1'})
      result = TimelineTable.all(:all, { :limit => 1 })
      result.size.should == 1
    end

    it 'select expression' do
      result = TimelineTable.all(:all, { :select_expression => 'user_id, author' })
      result[0]['user_id'].should_not == nil
      result[0]['body'].should == nil
    end

    it 'with block processing' do
      id_sum = 0
      TimelineTable.all { |element| id_sum += element['tweet_id'] }
      id_sum.should == 16
    end

    it 'one binding key' do
      result = TimelineTable.all(:all, { :where => ['user_id = ?', 'test_user0'] })
      result.size.should == 1
      result[0]['user_id'].should == 'test_user0'
    end

    it 'two binding keys' do
      result = TimelineTable.all(:all, { :where => ['user_id = ? and tweet_id = ?', 'test_user0', 16] })
      result.size.should == 1
      result[0]['user_id'].should == 'test_user0'
    end

    it 'few binding keys' do
      result = TimelineTable.all(:all, { :where => ['user_id = ? and tweet_id > ? and tweet_id < ?', 'test_user0', 10, 20] })
      result.size.should == 1
      result[0]['user_id'].should == 'test_user0'
    end

    it 'distinct values' do
      TimelineTable.create({'user_id' => 'test_user0', 'tweet_id' => 17, 'author' => 'test_author1', 'body' => 'test_body0'})
      TimelineTable.create({'user_id' => 'test_user1', 'tweet_id' => 16, 'author' => 'test_author2', 'body' => 'test_body0'})
      TimelineTable.create({'user_id' => 'test_user1', 'tweet_id' => 17, 'author' => 'test_author3', 'body' => 'test_body0'})

      TimelineTable.all('test_user0', {:where => ['tweet_id = ?', 17], :distinct => 'author'}).should == ['test_author1']
      TimelineTable.all('test_user1', {:distinct => 'tweet_id'}).sort.should == [16, 17]

    end
  end

  context 'count' do

    before(:each) do
      TimelineTable.create({'user_id' => 'test_user0', 'tweet_id' => 16, 'author' => 'test_author0', 'body' => 'test_body0'})
    end

    after(:each) do
      TimelineTable.truncate
    end

    it 'without params' do
      count = TimelineTable.count
      count.should == 1
    end

    it 'with key' do
      count = TimelineTable.count('test_user0')
      count.should == 1
    end

    it 'with not existing key' do
      count = TimelineTable.count('test_')
      count.should == 0
    end

    it 'with key and where clauses' do
      count = TimelineTable.count('test_user0', { :where => 'tweet_id >= 10' })
      count.should == 1
    end

    it 'with array key' do
      TimelineTable.create({'user_id' => 'test_user1', 'tweet_id' => 16, 'author' => 'test_author1', 'body' => 'test_body1'})
      count = TimelineTable.count(['test_user0', 'test_user1'])
      count.should == 2
    end

    it 'without key and with where clauses' do
      count = TimelineTable.count(:all, { :where => 'user_id = \'test_user0\'' })
      count.should == 1
    end

    it 'without key and with order clauses' do
      count = TimelineTable.count(:all, {:where => 'user_id = \'test_user0\'', :order => 'tweet_id' })
      count.should == 1
    end

    it 'without key and with limit clauses' do
      TimelineTable.create({'user_id' => 'test_user1', 'tweet_id' => 16, 'author' => 'test_author1', 'body' => 'test_body1'})
      count = TimelineTable.count(:all, { :limit => 1 })
      count.should == 1
    end

    it 'with block processing' do
      TimelineTable.create({'user_id' => 'test_user1', 'tweet_id' => 16, 'author' => 'test_author1', 'body' => 'test_body1'})
      id_sum = 0
      TimelineTable.count { |element| id_sum += element['count'] }
      id_sum.should == 2
    end

  end

  context 'create' do

    before (:each) do
      TimelineTable.truncate
    end

    after (:all) do
      TimelineTable.truncate
    end

    it 'create record' do
      TimelineTable.create({'user_id' => 'test_user0', 'tweet_id' => 0, 'author' => 'test_author0', 'body' => 'test_body0'})
      timelines = TimelineTable.all('test_user0')
      timelines.size.should == 1
      timelines[0]['user_id'].should == 'test_user0'
    end

    it 'timestamp' do
      TimelineTable.create({'user_id' => 'test_user0', 'tweet_id' => 0, 'author' => 'test_author0', 'body' => 'test_body0'}, { :timestamp => 2 })
      TimelineTable.create({'user_id' => 'test_user0', 'tweet_id' => 0, 'author' => 'test_author1', 'body' => 'test_body1'}, { :timestamp => 1 })
      TimelineTable.all.size.should == 1
      result = TimelineTable.all('test_user0')
      result.size.should == 1
      result[0]['author'].should == 'test_author0'
    end

    it 'ttl' do
      TimelineTable.create({'user_id' => 'test_user1', 'tweet_id' => 1, 'author' => 'test_author1', 'body' => 'test_body1'}, { :ttl => 1 })
      result = TimelineTable.all('test_user1')
      result.size.should == 1
      sleep(2)
      result = TimelineTable.all('test_user1')
      result.size.should == 0
    end

  end

  context 'update' do

    before (:each) do
      TimelineTable.truncate
    end

    after (:all) do
      TimelineTable.truncate
    end

    it 'update record' do
      TimelineTable.create({'user_id' => 'test_user0', 'tweet_id' => 0, 'author' => 'test_author0', 'body' => 'test_body0'})
      TimelineTable.update({'user_id' => 'test_user0', 'tweet_id' => 0, 'author' => 'test_author0', 'body' => 'test_body1'})
      result = TimelineTable.all('test_user0')
      result.size.should == 1
      result[0]['body'].should == 'test_body1'
    end

    it 'timestamp' do
      TimelineTable.create({'user_id' => 'test_user0', 'tweet_id' => 0, 'author' => 'test_author0', 'body' => 'test_body0'}, { :timestamp => 2 })
      TimelineTable.update({'user_id' => 'test_user0', 'tweet_id' => 0, 'author' => 'test_author0', 'body' => 'test_body1'}, { :timestamp => 1 })
      result = TimelineTable.all('test_user0')
      result.size.should == 1
      result[0]['author'].should == 'test_author0'
    end

    it 'ttl' do
      TimelineTable.update({'user_id' => 'test_user0', 'tweet_id' => 0, 'author' => 'test_author0', 'body' => 'test_body0'}, { :ttl => 1 })
      result = TimelineTable.all('test_user0')
      result.size.should == 1
      sleep(2)
      result = TimelineTable.all('test_user0')
      result.size.should == 0
    end

  end

  context 'delete' do
    before (:each) do
      TimelineTable.truncate
      TimelineTable.create({'user_id' => 'test_user0', 'tweet_id' => 0, 'author' => 'test_author0', 'body' => 'test_body0'})
      TimelineTable.create({'user_id' => 'test_user1', 'tweet_id' => 1, 'author' => 'test_author1', 'body' => 'test_body1'})
    end

    after (:each) do
     TimelineTable.delete('test_user0')
     TimelineTable.delete('test_user1')
    end

    it 'single key' do
      TimelineTable.delete('test_user0').should == true
      TimelineTable.all('test_user0').size.should == 0
    end

    it 'array key' do
      TimelineTable.delete(['test_user0', 'test_user1']).should == true
      TimelineTable.delete(['test_user0', 'test_user1']).should == true
      TimelineTable.all.size.should == 0
    end

    it 'supports bind' do
      TimelineTable.delete(nil, {:where => ['user_id = ? and tweet_id = ?', 'test_user0', '0']})
      TimelineTable.delete(nil, {:where => ['user_id = ? and tweet_id = ?', 'test_user1', '1']})
    end

    it 'single key with options' do
      TimelineTable.delete(nil, {:where => ['user_id = ? and tweet_id = ?', 'test_user0', '0'], :columns => ['author', 'body']})
      TimelineTable.all.size.should == 1
    end

    it 'supports timestamp' do
      TimelineTable.delete(nil, {:where => ['user_id = ? and tweet_id = ?', 'test_user0', '0'], :timestamp=> (Time.now.to_i + 10) }).should == true
      TimelineTable.all.size.should == 2
    end

  end

end

