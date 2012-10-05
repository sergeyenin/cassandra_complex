require 'spec_helper'

class TimelineModel < CassandraComplex::Model
  table 'timeline'

  attribute :user_id,  'varchar'
  attribute :tweet_id, 'int'
  attribute :author,   'varchar'
  attribute :body,     'varchar'

  primary_key :user_id, :tweet_id
end

CassandraComplex::Configuration.logger = Logger.new('/dev/null')

describe 'Model' do

  before :all do
    conn = CassandraComplex::Connection.new('127.0.0.1:9160')
    conn.execute('CREATE KEYSPACE cassandra_complex_test WITH strategy_class = \'SimpleStrategy\' AND strategy_options:replication_factor = 1;')
    CassandraComplex::Configuration.read({'host'=>'127.0.0.1:9160', 'default_keyspace'=>'cassandra_complex_test'})
    TimelineModel.create_table!
  end

  after :all do
    conn = CassandraComplex::Connection.new('127.0.0.1:9160')
    conn.execute('DROP TABLE timeline;')
    conn.execute('DROP KEYSPACE cassandra_complex;')
  end

  context 'basic operations' do
    it 'checks equality of two models' do
      timeline1 = TimelineModel.new({'user_id' => 'test_user1', 'tweet_id' => 1, 'author' => 'test_author1', 'body' => 'test_body1'})
      timeline2 = TimelineModel.new({'user_id' => 'test_user1', 'tweet_id' => 1, 'author' => 'test_author1', 'body' => 'test_body1'})

      timeline1.should == timeline2
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
      timeline_created = TimelineModel.create!({'user_id' => 'test_user1', 'tweet_id' => 1, 'author' => 'test_author1', 'body' => 'test_body1'})

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

      timeline.save!

      TimelineModel.all.size.should == 1
    end
  end

  context 'deleting model' do
    before (:each) do
      TimelineModel.truncate
      TimelineModel.create!({'user_id' => 'test_user1', 'tweet_id' => 1, 'author' => 'test_author1', 'body' => 'test_body1'})
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

end
