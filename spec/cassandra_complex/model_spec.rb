require 'spec_helper'

class TimelineModel < CassandraComplex::Model
  table 'timeline'

  attribute :user_id,  'varchar'
  attribute :tweet_id, 'int'
  attribute :author,   'varchar'
  attribute :body,     'varchar'

  primary_key :user_id, :tweet_id
end

class Tickets < CassandraComplex::Model
  table 'tickets'

  attribute :ticket_id, 'int'
  attribute :owner, 'varchar'
  attribute :time, 'timestamp'

  primary_key :ticket_id
end

CassandraComplex::Configuration.logger = Logger.new('/dev/null')

describe 'Model' do

  before :all do
    conn = CassandraComplex::Connection.new('127.0.0.1:9160')
    conn.execute('CREATE KEYSPACE cassandra_complex_test WITH strategy_class = \'SimpleStrategy\' AND strategy_options:replication_factor = 1;')
    CassandraComplex::Configuration.read({'host'=>'127.0.0.1:9160', 'default_keyspace'=>'cassandra_complex_test'})
    TimelineModel.create_table
    Tickets.create_table
  end

  after :all do
    conn = CassandraComplex::Connection.new('127.0.0.1:9160')
    TimelineModel.drop_table
    Tickets.drop_table
    conn.execute('DROP KEYSPACE cassandra_complex;')
  end

  context 'basic operations' do
    before :each do
      TimelineModel.truncate
      Tickets.truncate
    end

    it 'returns schema' do
      TimelineModel.schema.should == {:table => 'timeline', :attributes=>{:user_id => 'varchar', :tweet_id => 'int', :author => 'varchar', :body => 'varchar'}, :primary_key => [:user_id, :tweet_id]}
      Tickets.schema.should == {:table => 'tickets', :attributes=>{:ticket_id => 'int', :owner => 'varchar', :time => 'timestamp'}, :primary_key => [:ticket_id]}
    end

    it 'checks equality of two models' do
      timeline1 = TimelineModel.new({'user_id' => 'test_user1', 'tweet_id' => 1, 'author' => 'test_author1', 'body' => 'test_body1'})
      timeline2 = TimelineModel.new({'user_id' => 'test_user1', 'tweet_id' => 1, 'author' => 'test_author1', 'body' => 'test_body1'})

      timeline1.should == timeline2

      timeline3 = TimelineModel.new({'user_id' => 'test_user3', 'tweet_id' => 3, 'author' => 'test_author3', 'body' => 'test_body3'})
      timeline1.should_not == timeline3

      tickets = Tickets.new({:ticket_id => 1, :owner=> 'Tim Collins', :time => Time.now})

      tickets.should_not == timeline3
    end

    it 'implements dirtiness' do
      timeline1 = TimelineModel.new({'user_id' => 'test_user1', 'tweet_id' => 1, 'author' => 'test_author1', 'body' => 'test_body1'})
      timeline1.dirty?.should == true

      timeline1.save
      timeline1.dirty?.should == false

      timeline1 == TimelineModel.all[0]
      timeline1.dirty?.should == false
      timeline1.author = 'test_author42'
      timeline1.dirty?.should == true
    end
  end

  context 'creating new model' do
    before (:each) do
      TimelineModel.truncate
    end

    it 'with Hash' do
      timeline = TimelineModel.new({'user_id' => 'test_user1', 'tweet_id' => 1, 'author' => 'test_author1', 'body' => 'test_body1'})

      timeline.user_id.should == 'test_user1'
      timeline.tweet_id.should == 1
      timeline.author.should == 'test_author1'
      timeline.body.should == 'test_body1'
    end

    it 'with assigment' do
      timeline = TimelineModel.new

      timeline.user_id  = 'test_user1'
      timeline.tweet_id = 1
      timeline.author   = 'test_author1'
      timeline.body     = 'test_body1'

      timeline.user_id.should == 'test_user1'
      timeline.tweet_id.should == 1
      timeline.author.should == 'test_author1'
      timeline.body.should == 'test_body1'
    end

    it 'creating and saving new model within DB' do
      timeline_created = TimelineModel.create({'user_id' => 'test_user1', 'tweet_id' => 1, 'author' => 'test_author1', 'body' => 'test_body1'})

      timeline_created.user_id.should == 'test_user1'
      timeline_created.tweet_id.should == 1
      timeline_created.author.should == 'test_author1'
      timeline_created.body.should == 'test_body1'

      timeline_found = TimelineModel.find('test_user1', :where=>['tweet_id = ?', 1])
      timeline_created.should == timeline_found.first
    end
  end

  context 'saving model' do

    before (:each) do
      TimelineModel.truncate
    end

    it 'just save' do
      timeline = TimelineModel.new({'user_id' => 'test_user1', 'tweet_id' => 1, 'author' => 'test_author1', 'body' => 'test_body1'})

      timeline.save

      TimelineModel.all.size.should == 1
    end
  end

  context 'deleting model' do
    before (:each) do
      TimelineModel.truncate
      TimelineModel.create({'user_id' => 'test_user1', 'tweet_id' => 1, 'author' => 'test_author1', 'body' => 'test_body1'})
    end

    it 'as class method' do
      TimelineModel.delete({'user_id' => 'test_user1', 'tweet_id' => 1})
      TimelineModel.all.size.should == 0
    end

    it 'as instance method' do
      TimelineModel.find('test_user1', :where=>['tweet_id = ?', 1])[0].delete
      TimelineModel.all.size.should == 0
    end

  end

  context 'all, find, count, etc' do

    before (:each) do
      TimelineModel.truncate
      TimelineModel.create({'user_id' => 'test_user0', 'tweet_id' => 0, 'author' => 'test_author0', 'body' => 'test_body0'})
      TimelineModel.create({'user_id' => 'test_user0', 'tweet_id' => 1, 'author' => 'test_author1', 'body' => 'test_body1'})
      TimelineModel.create({'user_id' => 'test_user2', 'tweet_id' => 2, 'author' => 'test_author2', 'body' => 'test_body2'})
      TimelineModel.create({'user_id' => 'test_user3', 'tweet_id' => 3, 'author' => 'test_author3', 'body' => 'test_body3'})
    end


    it 'count' do
      TimelineModel.count.should == 4
      TimelineModel.count('test_user0').should == 2
      TimelineModel.count(:all, {:where=>['user_id = ? and tweet_id = ?', 'test_user0', 0]}).should == 1
      TimelineModel.count({'user_id' => 'test_user0'}).should == 2
    end

    it 'all' do
      TimelineModel.all.size.should == 4
      TimelineModel.all({:where=>['user_id = ?', 'test_user0']}).size.should == 2
    end

    it 'find' do
      TimelineModel.find(:all).size.should == 4
      TimelineModel.find(:all, {:where=>['user_id = ?', 'test_user0']}).size.should == 2
      TimelineModel.find({'user_id' => 'test_user0', 'tweet_id' => 1}).size.should  == 1
    end

  end

end
