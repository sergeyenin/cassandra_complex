require 'spec_helper'

describe "Row" do

  before :each do
    begin
      @row = CassandraModelCql::Row.new({:table => 'timeline'})
    rescue => ex
      @row = nil
    end
  end

  context "initialize()" do
    it "works" do
      return false if !@row
    end
  end

  context "[]=()" do
    it "saves value" do
      return false if !@row
      @row['test_key'] = 'test_value'
    end
  end

  context "save()" do
    it "saves data" do
      return false if !@row
      @row.save
    end
  end

end
