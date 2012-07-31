module CassandraModelCql
  # ActiveRecord like sugared model
  # @example Using model
  # class Timeline < CassandraModelCql::Model
  #
  #   set_keyspace 'history'
  #
  #   attribute :user_id, 'varchar'
  #   attribute :tweet_id, 'uuid'
  #   attribute :author, 'varchar'
  #   attribute :body, 'varchar'
  #   #composite primary key
  #   primary_key :user_id, :tweet_id
  # end
  #
  # #creating Column Family
  # Timeline.create_table
  #
  # t = Timeline.new(:user_id=>'mickey', :tweet_id=>1715, :author=> 'mouse', :body=>"'Hello!'")
  # t.save
  #
  # timelines = Timeline.all('mickey')
  # t = timelines.first
  # puts t.body
  # => 'Hello!'
  # t.body  = "'Goodbye!'"
  # puts t.dirty?
  # => true
  # t.save
  #
  # t.update(:tweet_id=>1777)
  #
  # #dropping Column Family
  # Timeline.drop_table
  class Model < Table
  end
end
