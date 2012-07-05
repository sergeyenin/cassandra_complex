require 'spec_helper'

describe "Column" do

  context "initialize()" do

    it "works" do
      begin
        require 'cassandra_model_cql/column'
      rescue => ex
       raise 'Cannot load the module'
      end
    end

  end

end
