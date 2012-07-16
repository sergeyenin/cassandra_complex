require 'spec_helper'

class TestTable < CassandraModelCql::Model
end

describe "Model" do

  before :all do
    conn = CassandraModelCql::Connection.new('127.0.0.1:9160')
    conn.execute('CREATE KEYSPACE test_spec1 WITH strategy_class = \'SimpleStrategy\' AND strategy_options:replication_factor = 1;')
  end

  #after :all do
    #conn = CassandraModelCql::Connection.new('127.0.0.1:9160')
    #conn.execute('DROP KEYSPACE test_spec1;')
  #end

  context 'create model' do

    before :all do
      @t = TestTable.new
    end

    it 'attribute' do
      @t.attribute(:user_id, 'varchar')
      @t.last_error.should == nil
      @t.user_id = 'test'
      @t.user_id.should == 'test'
    end

    it 'primary key' do
      @t.primary_key(:user_id)
      @t.last_error.should == nil
    end

    it 'keyspace' do
      @t.set_keyspace('test_spec1')
      @t.keyspace.should == 'test_spec1'
    end

  end

  context 'create table' do
    it 'works' do
      TestTable.create_table
      TestTable.last_error.should == nil
    end
  end

  context 'insert record' do

    it 'works with valid  params' do
      t = TestTable.new(:user_id=>"'user0'", :tweet_id=>"1715", :author=> "'author0'", :body=>"'Hello!'")
      t.save
      t.last_error.should == nil
      t = TestTable.new(:user_id=>"'user1'", :tweet_id=>"1716", :author=> "'author1'", :body=>"'Hello!'")
      t.save
      t.last_error.should == nil
      t = TestTable.new(:user_id=>"'user2'", :tweet_id=>"1717", :author=> "'author2'", :body=>"'Hello!'")
      t.save
      t.last_error.should == nil
      t = TestTable.new(:user_id=>"'user3'", :tweet_id=>"1715", :author=> "'author3'", :body=>"'Hello!'")
      t.save
      t.last_error.should == nil
      t = TestTable.new(:user_id=>"'user4'", :tweet_id=>"1716", :author=> "'author4'", :body=>"'Hello!'")
      t.save
      t.last_error.should == nil
    end

    it 'returns error if there is not primary key' do
      begin
        t = TestTable.new( :tweet_id=>"1715", :author=> "'author0'", :body=>"'Hello!'")
        t.save
        t.last_error.should != nil
      rescue
        true
      end
    end

    it 'returns error if there is invalid type' do
      begin
        t = TestTable.new(:user_id => "'user0'", :tweet_id=>"'1715'", :author=> "'author0'", :body=>"'Hello!'")
        t.save
        t.last_error.should != nil
      rescue
        true
      end
    end

  end

  context 'find' do

    it 'without params' do
      result = TestTable.find
      TestTable.last_error == nil
      result.size.should == 5
    end

    it 'with key' do
      result = TestTable.find('user0')
      TestTable.last_error.should == nil
      result.size.should == 1
      result.first.user_id.should == 'user0'
    end

    it 'with not existing key' do
      result = TestTable.find('test_')
      TestTable.last_error.should == nil
      result.size.should == 0
    end

  end

  context 'change' do

    it 'keeps value and sets dirty flag' do
      t = TestTable.new
      t.user_id = 'user5'
      t.dirty?.should == true
    end

    it 'create record' do
      t = TestTable.new
      t.user_id = 'user5'
      t.tweet_id = 5
      t.author = 'author5'
      t.body = 'body5'
      t.save
      t.last_error.should == nil
      result = TestTable.find('user5')
      TestTable.last_error.should == nil
      result.size.should == 1
      result.first.user_id.should == 'user5'
    end

    it 'update record' do
      t = TestTable.find('user0').first
      TestTable.last_error.should == nil
      t.body = 'body0'
      t.save
      t.last_error.should == nil
      TestTable.find('user0').first.body.should == 'body0'
    end

  end

  context 'drop table' do

    it 'successful drop of existing table' do
      TestTable.drop_table
    end

    it 'fail drop of nonexisting table' do
      begin
        TestTable.drop_table
        TestTable.last_error.should != nil
      rescue
        true
      end
    end

  end

end

