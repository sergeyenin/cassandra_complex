module CassandraComplex
  # A composite index for table
  # @example
  # class Timeline < CassandraComplex::Model
  #
  #   set_keyspace 'history'
  #
  #   attribute :user_id, 'varchar'
  #   attribute :author_tweet_id, 'uuid'
  #   attribute :author, 'varchar'
  #   attribute :body, 'varchar'
  #
  #   primary_key :user_id, :tweet_id
  # end
  #
  # class Tweet < CassandraComplex::Model
  #   set_keyspace 'history'
  #
  #   attribute :tweet_id, 'varchar'
  #   attribute :important_data, 'varchar'
  # end
  #
  # class TwitIndex < CassandraComplex::Index
  #   index :with_table=>Timeline  #, :attributes=>[:author, :author_tweet_id]
  #   indexing :table=> Tweet, :index=>[:tweet_id]
  #
  #   indexing_rule Proc.new{|timeline| timeline.author.to_s + ':' + timeline.author_tweet_id.to_s}
  # end
  #
  # t = Timeline.find('gmason')
  # puts t.inspect
  # => <Timeline:{:user_id=>'gmason', :author_tweet_id=>1, :author=>'gwashington', :body=>'Some text'}
  # puts t.tweets.inspect
  # <Tweet:{:tweet_id=>'gwashington:1', :important_data=>'Some data.'}>

  class Index < Table
  end
end
