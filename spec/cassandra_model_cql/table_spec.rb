require 'spec_helper'

class Timeline < CassandraModelCql::Table
end

describe "Table" do

  context "connection" do
    it "works" do
      Timeline.connection('history')
    end
  end
    #rows = Timeline.all('some_primary_key')
    #rows.each do |row|
      #row['body'] = 'Another body!'
      #row.save
    #end

  context "table_name"  do
    it "returns correct table name" do
      Timeline.table_name.should == 'timeline'
    end
  end

  context "primary_key" do
    it "returns value" do
      Timeline.connection('history')
      puts Timeline.primary_key
    end
  end

end

