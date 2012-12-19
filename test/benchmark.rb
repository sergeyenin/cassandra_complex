require 'progress_bar'
require 'benchmark'

MAX = 10**6
PORTION = MAX / 100

class TimelineModel < CassandraComplex::Model
  table 'timeline'

  attribute :user_id,  'varchar'
  attribute :tweet_id, 'int'
  attribute :author,   'varchar'
  attribute :body,     'varchar'

  primary_key :user_id, :tweet_id
end

progress_bar = ProgressBar.new(MAX, :bar, :percentage, :elapsed, :eta)

conn = CassandraComplex::Connection.new('127.0.0.1:9160')
conn.execute('DROP KEYSPACE cassandra_complex_benchmark;')
conn.execute('CREATE KEYSPACE cassandra_complex_benchmark WITH strategy_class = \'SimpleStrategy\' AND strategy_options:replication_factor = 1;')
CassandraComplex::Configuration.read({'host'=>'127.0.0.1:9160', 'default_keyspace'=>'cassandra_complex_benchmark'})
TimelineModel.create_table
puts "#{MAX} records created, found, deleted within cassandra in(user, system, total, real) time:"
puts Benchmark.measure{
  (1..MAX).each do |i|

    progress_bar.increment! PORTION if (i%PORTION) == 0
    id = "test_#{i}"
    TimelineModel.new({'user_id' => id, 'tweet_id' => i, 'author' => id, 'body' => id}).save
    timeline = TimelineModel.find(id)
    timeline[0].delete
  end
}

TimelineModel.drop_table
conn.execute('DROP KEYSPACE cassandra_complex_benchmark;')
