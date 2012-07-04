# cassandra\_model\_cql

Rought Wrapper for Cassandra CQL operations.

## Example of usage

    class Timeline < CassandraModelCql::Table
    end

    rows = Timeline.all('some_primary_key')
    rows.each do |row|
      row['body'] = 'Another body!'
      row.save
    end

## Most important classes within CassandraModelCql

### Table

Class Table implement ORM like interface to access rows by primary key.

### RowSet

Instance of class RowSet is returned as result of any Cassandra operation.
It is provides access to last_error and last_error_command.
RowSet is Enumerable by rows.

### Row

Row is hash like accessable row of Cassandra operation result.
It could be changed and saved.

### Column

Column implement Cassandra column and has column name, value and timestamp.
Could be dirty if changed and not saved.

### Connection
Class Connection encapsulate Cassandra driver and execute CQL3 with it.
