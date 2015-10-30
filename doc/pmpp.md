# Poor Man's Parallel Processing (PMPP) 


PMPP is an extension for executing multiple statements in parallel. Those statements might be completely independent, or they might need to be further combined and aggregated. In either case, it is up to the user to decide how a larger query should be broken into smaller ones.


## Common Strategies for using PMPP

Before delving into to specifics of functions and types provided by PMPP, it is helpful to first see some examples of problems solved by PMPP.

### System information

Get a list of all tables in a given schema, and the row count of each of those tables. Show the largest tables first.

```sql
CREATE TEMPORARY TABLE table_counts(table_name text, row_count bigint);
SELECT *
FROM pmpp.distribute(null::table_counts,
                    'dbname=' || current_database(),
                    array ( SELECT format('SELECT %L, count(*) from %I',t.table_name,t.table_name)
                            FROM information_schema.tables t
                            WHERE t.table_schema = 'public' ))
ORDER BY row_count DESC;
 table_name | row_count
------------+-----------
 foo        |        10
 a          |         0
(2 rows)
```

This could have been accomplished via a series of `SELECT COUNT(*) from foo` statements, but then those results would have had to be loaded into a manually created table before being re-queried.

### Two-stage aggregation of partitions

Assume that there is a table user_views with partitions for a given number of months. If the following query is run:
```sql
SELECT user_name, sum(view_count) as total_view_count
FROM user_views
GROUP BY 1
ORDER BY 2
```

This query will scan all of the partitions in sequence, and then aggregate the entire set. 

```sql
SELECT user_name, sum(view_count) as total_view_count
FROM pmpp.distribute(null::user_views,
                    'dbname=' || current_database(),
                    array[  'SELECT user_name, sum(view_count) FROM user_views_december GROUP BY 1',
                            'SELECT user_name, sum(view_count) FROM user_views_november GROUP BY 1',
                            'SELECT user_name, sum(view_count) FROM user_views_october GROUP BY 1',
                            'SELECT user_name, sum(view_count) FROM user_views_september GROUP BY 1' ])
GROUP BY 1
ORDER BY 2 DESC
```

With this query, the individual partial aggregate queries will be run independently, some simultaneously (depending on how many CPUs are available and any limiting parameters specified), and those results will then be returned by the set returning function `pmpp.distribute()` which can itself be queried to obtain the final aggregate.


### Parallel index Building.

Build all indexes for a table at the same time, up to the number of CPUs on the db machine.

```
CREATE TABLE parallel_index_test( b integer, c integer, d integer);
SELECT  *
FROM pmpp.meta('dbname=' || current_database(),
               array( SELECT format('create index on %s(%s)',c.table_name,c.column_name)
                      FROM information_schema.columns c
                      WHERE c.table_name = 'parallel_index_test'
                      AND c.table_schema = 'public'))
ORDER BY 1;
                command                 | result
----------------------------------------+--------
 create index on parallel_index_test(b) | OK
 create index on parallel_index_test(c) | OK
 create index on parallel_index_test(d) | OK
(3 rows)
```


### Hiding passwords

Even if foreign data wrappers are not used, it is helpful to have [postgresql_fdw](http://www.postgresql.org/docs/current/static/postgres-fdw.html) installed. This makes it possible to leverage foreign server definitions and user mappings makes for cleaner function invocations.

```sql
SELECT current_database() as mydb, current_user as me
\gset
CREATE EXTENSION postgres_fdw;
CREATE SERVER loopback FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host 'localhost', port '5432', dbname :'mydb');
CREATE USER MAPPING FOR :me SERVER loopback OPTIONS (user :'me', password 'REDACTED');


CREATE TEMPORARY TABLE my_result(x integer);
SELECT *
FROM pmpp.distribute(null::my_result,'loopback',array['SELECT 1','SELECT 2']);
 x
---
 1
 2
(2 rows)
```

Clearly, this does not solve the issue of storing passwords, but it does isolate the issue to the `pg_user_mappings` table.


## Functions

### distribute() - single database

PMPP relies heavily on polymorphic functions returning SETOF anyelement. This allows the user to define the result set that a set of queries will return, rather than have a separate function for every possible result set. This comes at a cost, however, in that the database must have a existing type that matches the result set shape desired. This type can be an explicit CREATE TYPE, or it can be the type of an existing table. Often, the type is a temporary table created at runtime to match an ad-hoc query.

```sql
function distribute( 
            row_type anyelement,
            connection text,
            sql_list text[],
            cpu_multiplier float default 1.0,
            num_workers integer default null,
            statement_timeout integer default null) returns setof anyelement
```

This is the simplest form of the `distribute()` function, geared more towards local parallelism rather than sharding.

#### Parameters:

* **row_type**: a null value casted to the type or table which specifies the shape of the result set that will be returned by this function. The result sets of all queries in `sql_list` will be coerced to fit this result set, if possible. If not possible, then the function will fail.
* **connection**: a text string that represents a valid connection_string to dblink(). It can be any valid connection string that could be passed to `psql` on the command line, or it can be the name of a foreign server already defined on the local database. It is passed as-is to `dblink_connect`, so if dblink can use it, it's valid.
* **sql_list**: an array of text, each element being an executable statement, almost always a SELECT, though any statement that can be executed over dblink() is possible. Each element of the array must be valid SQL, and it must reference objects found on the connection `connection`. The result set generated needs to be coercable into `row_type`, though it should be noted that only column order and (approximate, coercable) type matter, column names do not matter. 
* **cpu_multiplier** (often omitted, defaults to 1.0): Specifies the number of connections to make to `connection` relative to the number of CPUs found on that machine, but never less than one or more than the number of array elements in `queries`. A value of 2.0 means to make two connections per CPU, 0.25 means one connection per four CPUs, etc. This value is trumped by `num_workers`. If this value is specified (and `num_workers` is not) then the connection `connection` *must* have PMPP installed.
* **num_workers** (often omitted/null): If set, specifies the max number of connections to `connection` to make. PMPP will never make more connections than there are queries to run for that `connection`. Specifying this value overrides any value in `cpu_multiplier`. This value is requires if the database at `connection` does not have PMPP installed.
* **statement_timeout** (often omitted/null): If set, each connection to `connection` has `SET statement_timeout=<value>` executed after the connection is established.

The number of connections created is never less than one, and never more than the number of queries in `sql_list`. When a query completes, its results are passed along to the output result set, and the function will give the connection another query, if any remain. When no more queries are available for distribution, the connection will be disconnected. When all connections have disconnected, the function returns the combined result set.


#### Example

```
CREATE TEMPORARY TABLE x(y integer);
CREATE TABLE
SELECT *
FROM pmpp.distribute(null::x,'dbname=mydb',array['select 1','select 2','select 3 as z']);
 y
---
 1
 2
 3
(3 rows)
```

In this example, a table was defined, but never populated. Rather, it was defined because it matched the shape of the result sets we would expect from all of the queries in the array. It should be noted that the name of the columns do not matter, only the order and datatypes. A common usage is to query a partitioned table foo, specifying `null::foo` as the table type, and the queries in the array would be queries on the individual partitions of foo. The remote connection does not need to have the object `x` defined, only the local connection does.

The function connected as the superuser of a local database named mydb. Connecting as superuser is rarely ideal. The distribute() function connected to mydb, determined the number of available processors there (which means that mydb must also have PMPP installed - there are ways around that discussed in `num_workers`.

### distribute() - query_manifest[]

```sql
function distribute(row_type anyelement, query_manifest query_manifest[]) returns setof anyelement
```

The type `query_manifest` contains the following attributes
```sql
type query_manifest as ( 
         connection text, 
         queries text[], 
         num_workers integer, 
         cpu_multiplier float, 
         statement_timeout integer );
```

Each of these attributes exactly matches the purpose described by the same-named parameters in the single database version of `distribute()`.

For each `query_manifest` object in the array:
1. Connect to each of the databases with a number of connections equal to num_workers (if specified), or the number of cpus found on the machine at the end of that connection times the mulitplier (defaults to 1.0).
2. Each query specified must return a result set matching the type `row_type`.
3. The queries are distributed to the connections as they become available, and the results of those queries are in turn pushed to the result set, where they can be further manipulated like any table, view, or set returning function.
4. Returns a result set in the shape of `row_type`.

#### Example

```
CREATE TEMPORARY TABLE xy(x integer, y integer);
SELECT * 
FROM pmpp.distribute(
        null::xy,
        array[ row('dbname=mydb', 
                    array['SELECT 1,2','SELECT 3,4'],
                    null,
                    4,
                    null)::pmpp.query_manifest,
               row('named_foreign_server',
                    array['SELECT 5,6','SELECT 7,8'],
                    1.5,
                    null,
                    null)::pmpp.query_manifest,
               row('postgresql://user:secret@localhost',
                    array['SELECT 9,10'],
                    null,
                    null,
                    null)::pmpp.query_manifest ])
ORDER by x;
 x | y
---+----
 1 |  2
 3 |  4
 5 |  6
 7 |  8
 9 | 10
(1 row)
```

Clearly this isn't the easiest structure to manage, it's likely that if this form is used directly at all, it would be invoked from a parent function.

Notice the variety of connection strings used: 
* A superuser connection to a local db.
* The name of a server defined in `pg_foreign_server`, which must also ahave a proper user mapping for the invoking user.
* A qualified postgres URI.

Also note the following:
* The first query_manifest row has `num_workers=4` specified. This means that `distribute()` would not ask the connection how many CPUs it has, but would instead just make 4 connections. However, since there are only 2 queries in the list, only 2 connections will be made.
* The second query_manifest row has `cpu_multiplier=1.5`. This means that `distribute()` will invoke `pmpp.num_cpus()` on one of the remote connections to determine how many CPUs that machine has, and the function will fail if PMPP is not installed at that connection. Assuming the remote machine has 4 CPUs, a multiplier of 1.5 would mean that PMPP would try to make 6 connections. Again, however, there are only 2 queries specified, so only 2 connections would be made.
    

### distribute() - jsonb/json versions

```sql
function distribute( p_row_type anyelement, p_query_manifest jsonb ) returns setof anyelement
function distribute( p_row_type anyelement, p_query_manifest json ) returns setof anyelement
```

These are essentially the same as the query_manifest[] version, but the commands are encoded as single JSON[B] document. The JSON[B] structure is just an array of elements, each element being a collection of attributes with the same names as the attributes in type `query_manifest`. Omitted elements are left null.    

#### Example

```
CREATE TEMPORARY TABLE xy(x integer, y integer);
SELECT * 
FROM pmpp.distribute(
        null::xy,
        '[{"connection": "foreign_server_without_pmpp", "queries": ["SELECT 1,2","SELECT 3,4"], "num_workers": 4},'
        ' {"connection": "named_foreign_server", "queries": ["SELECT 5,6", "SELECT 7,8"], "multiplier": 1.5},'
        ' {"connection": "postgresql://user:secret@localhost", "query": "SELECT 9,10"}]'::jsonb)
ORDER by x;
 x | y
---+----
 1 |  2
 3 |  4
 5 |  6
 7 |  8
 9 | 10
(1 row)
```





### meta()

Given a single connection string, execute a series of statements (ones that don't return sets) in parallel.

```sql
function meta(
            connection text,
            sql_list text[],
            cpu_multiplier float default null,
            num_workers integer default null,
            statement_timeout integer default null) returns setof command_with_result
```

Each statement in `sql_list` is wrapped in a `SELECT pmpp.execute_command(<statement>)`, which returns a type `command_with_result` which looks like this: 

```sql
type command_with_result ( command text, result text );
```

#### Example:

```
CREATE TABLE a( b integer, c integer, d integer);
CREATE TABLE
SELECT  *
FROM pmpp.meta('dbname=' || current_database(),
               array( SELECT format('create index on %s(%s)',c.table_name,c.column_name)
                      FROM information_schema.columns c
                      WHERE c.table_name = 'parallel_index_test'
                      AND c.table_schema = 'public'))
ORDER BY 1;
                command                 | result
----------------------------------------+--------
 create index on parallel_index_test(b) | OK
 create index on parallel_index_test(c) | OK
 create index on parallel_index_test(d) | OK
(3 rows)
```

This example has the user connecting to the same database as a local superuser (normally not a good idea) and creating an index on every column in the table. The database connected to must also have PMPP installed.

### broadcast()

Given a single connection string, execute a series of statements (ones that don't return sets) in parallel.

```sql
function broadcast(
            row_type anytype,
            connections text[],
            query text) returns setof anyelement
```

Run the query on every connection in the connections list.

#### Example:

```sql
CREATE TEMPORARY TABLE x( y integer );
CREATE TABLE
SELECT  *
FROM pmpp.broadcast(null::x,
                    array['connection_1','connection_2'],
                    'select 1');

 y
---
 1
 1
```


### Other functions

#### execute_command

```sql
function execute_command(sql text) RETURNS command_with_result
```

Executes the command `sql` and returns that string along with a returncode of `'OK'` or `'FAIL'`. 

This function is used inside `meta()`. It is not particularly useful on its own.

##### Example:

```
SELECT * from pmpp.execute_command('analyze');
 command | result
---------+--------
 analyze | OK
(1 row)
```

### Internal functions of no direct utility to the user

Presented mostly to alleviate curiosity.

#### jsonb_array_to_text_array

```sql
function jsonb_array_to_text_array(jsonb jsonb) returns text[]
```

PostgreSQL current has no function to convert direction from a JSONB array to a text[].

#### num_cpus

```sql
function num_cpus() RETURNS integer
```

Returns the number of CPUs detected on this machine at the time the extension was added.

```sql
function num_cpus(multiplier float) RETURNS integer
```

Returns `multiplier * num_cpus()`, but never less than 1. 


#### disconnect
```sql
function disconnect() returns setof text
```

Cancels all active queries and disconnects from all connections created by PMPP. This is really only useful if the query dies and PMPP somehow fails to clean up after itself.    

#### query_manifest functions

```sql
function to_query_manifest(item in jsonb) returns query_manifest
```

The function behind the `jsonb` to `query_manifest` cast.


```sql
function to_query_manifest_array(manifest in jsonb) returns query_manifest[]
```

The function behind the `jsonb` to `query_manifest[]` cast.

```sql
function manifest_set( query_manifest jsonb ) returns setof query_manifest
```

Extracts a set of `query_manifest` records from a `jsonb` object.

```sql
function manifest_array( query_manifest jsonb ) returns query_manifest[]
```

Extracts an array of `query_manifest` records from a `jsonb` object.

## Considerations

* Objects and data created by the parent connection but not yet committed are **not** visible to the connections created by PMPP.
* The connection specified may not be the same user as the calling function, and thus may not have the same object permissions as the parent connection.
* If a sub-query fails, PMPP will *try* to cancel all other sub-queries and disconnect from those connections.
* Replica databases are completely read-only, so it is not possible to create temporary tables on them.
* In situations where the remote machine isn't a "real" PostgreSQL instance, but instead another database that happens to use the libpq protocol (Redshift, Vertica), the `num_workers` parameter *must* be specified.
* When querying a database that has it's own massively-parallel engine (Redshift, Vertica), there is no point in starting more than one worker per server, so set `num_workers=1`.


## Under The Hood.

Parallel querying is accomplished through usage of the async funcions in the [dblink](http://www.postgresql.org/docs/current/static/dblink.html) extension. Queue management is currently implemented in temp tables with plpgsql, though it may be necessary to recode it in C due to some shortcomings of pl/pgsql.

## Wishlist

A few things that could make PMPP better.

* Polymorphic `dblink()` functions or `TYPE`ing of `SETOF RECORD` functions.
    The function call to `dblink_get_result()` is dynamic because the typecast is not known until runtime.
    This is being worked on for PostgreSQL 9.6.

* Ability to fetch `PQcmdStatus(PGresult *res)` from PL/PGSQL

    Currently only success or failure is detected.

### Support

Submit issues to the [GitHub issue tracker](https://github.com/moat/pmpp/issues).

### Author

Corey Huinker, while working at [Moat](http://moat.com)

### Copyright and License

Copyright (c) 2015, Moat Inc.

Permission to use, copy, modify, and distribute this software and its documentation for any purpose, without fee, and without a written agreement is hereby granted, provided that the above copyright notice and this paragraph and the following two paragraphs appear in all copies.

IN NO EVENT SHALL MOAT INC. BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF Moat, Inc. HAS BEEN ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

MOAT INC. SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS IS" BASIS, AND Moat, Inc. HAS NO OBLIGATIONS TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.



