require 'spec_helper'

describe "Row Set" do

  before :each do
    @conn = CassandraModelCql::Connection.new('127.0.0.1:9160',{:keyspace => 'History'})
  end

  context "initialize()" do

    it "creates object with table param" do
      row_set = CassandraModelCql::RowSet.new(@conn,'timeline')
    end

    it "creates object without table param" do
      row_set = CassandraModelCql::RowSet.new(@conn,nil)
    end

    it "creates object without connection" do
      row_set = CassandraModelCql::RowSet.new(nil,'timeline')
    end

  end

  context "execute_query()" do
    it "gets data" do
      @row_set = CassandraModelCql::RowSet.new(@conn,'timeline')
      @row_set.execute_query("select * from timeline where body = 'test_body'")
    end
  end

  context "each()" do
    it "works" do
      @row_set = CassandraModelCql::RowSet.new(@conn,'timeline')
      @row_set.execute_query("select * from timeline where body = 'test_body'")
      @row_set.each {|element| puts element.inspect}
    end
  end

  context "last_command()" do
    it "keeps" do
      @row_set = CassandraModelCql::RowSet.new(@conn,'timeline')
      @row_set.execute_query("select * from timeline where body = 'test_body'")
      @row_set.last_command.should == "select * from timeline where body = 'test_body'"
    end
  end

end
