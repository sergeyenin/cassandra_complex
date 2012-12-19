# cassandra\_complex

Basic model - wrapper for CQL(Cassandra Query Language) operations.

## Extern interfaces provided by CassandraModelCql

### Table

Class Table implements wrapping of CQL3 operations.

### Model

Class Model implements basic model.

## Example of usage

### Basic configuring

    CassandraComplex::Configuration.read({'host'=>'127.0.0.1:9160', 'default_keyspace'=>'cassandra_complex_test'})
    CassandraComplex::Configuration.logger = Logger.new(STDOUT)

### Using interface provided by CassandraComplex::Table

    class Timeline < CassandraComplex::Table
      set_table_name 'timeline'
    end
    Timeline.create({'user_id' => 'test_user0', 'tweet_id' => 16, 'author' => 'test_author0', 'body' => 'test_body0'})
    Timeline.all
    Timeline.delete('test_user0')
    Timeline.truncate

### Using Model interface

    class TimelineModel < CassandraComplex::Model
      table 'timeline'
      attribute :user_id,  'varchar'
      attribute :tweet_id, 'int'
      attribute :author,   'varchar'
      attribute :body,     'varchar'
      primary_key :user_id, :tweet_id
    end
    Timeline.create_table
    timeline1 = TimelineModel.new({'user_id' => 'test_user1', 'tweet_id' => 1, 'author' => 'test_author1', 'body' => 'test_body1'})
    timeline1.author = 'test_author42'
    timeline1.dirty? == true
    timeline1.save
    timeline1.dirty? == false
    Timeline.drop_table

## Features

* Wrapping all CQL3 operations, no need to write any CQL3 code.

* Model provides basic model with dirtiness.

* All selects, such as .all, .find, count returns arrays of pure hashes.

* All other operations return true if success or false otherwise.

* You can iterate through result row at moment it being fetched from Cassandra.

* You can execute any operation within context of Table.with_keyspace.

* All connections operations to each keyspace is protected with mutex.
