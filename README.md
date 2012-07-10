# cassandra\_model\_cql

Helper - Wrapper for Cassandra CQL operations.

## Example of usage

    class Timeline < CassandraModelCql::Table
      set_keyspace 'history'
    end
    # each row is being processed, while it is fetched from Cassandra
    Timeline.all('some_primary_key') do
      row['body'] = 'Another body!'
      row.save
    end

## Features

* Wrapping all CQL3 operations, no need to write any CQL3 code.

* In case of any error Table` child provide .last_error and last_error_command.

* All selects, such as .all, .find, count returns arrays of pure hashes.

* All other operations return true if success or false otherwise.

* You can iterate through result row at moment it being fetched from Cassandra.

* You can execute any operation within context of Table.with_keyspace.

* All connections operations to each keyspace is protected with mutex.

## Extern interfaces provided by CassandraModelCql

### Table

Class Table implement wrapping of CQL3 operations.
