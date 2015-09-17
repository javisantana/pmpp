

\echo roles must exist on both sides of the connection so they cannot be created in a transaction
\set pmpp_test_user 'pmpp_test_user'
\set password 'dummy';
\set nopw_test_user 'nopw_test_user'
create role :pmpp_test_user password :'password' login;
create role :nopw_test_user password '' login;
grant pmpp to :pmpp_test_user, :nopw_test_user;
create schema dblink_test_schema;
create extension dblink schema dblink_test_schema;
create extension postgres_fdw; -- a user-mappings convenience, not a dependency
create extension pmpp;

select  current_database() as dbname,
        'dbname=' || current_database() as loopback_su_conn_str
\gset 


\set pmpp_localhost_server 'localhost_server'

select  format('[{"connection": "%s", "query": "select 1"}]',
                :'loopback_su_conn_str') as json_str_1,
        format('[{"connection": "%s", "query": "select 999"}]',
                :'pmpp_localhost_server') as json_str_2,
        format('[{"connection": "%s", "query": "select count(pg_sleep(1))", "statement_timeout": 100}]',
                :'pmpp_localhost_server') as json_str_timeout,
        format('[{"connection": "%s", "num_workers": 1, "queries": ["select 1","select 2/0","select 3"]}]',
                :'pmpp_localhost_server') as json_str_div_zero
\gset 

----
-- test disconnect()
select  dblink_test_schema.dblink_connect('pmpp.999',:'loopback_su_conn_str');
select *
from unnest(dblink_test_schema.dblink_get_connections()) as c;
select  pmpp.disconnect();
select *
from unnest(dblink_test_schema.dblink_get_connections()) as c;


select  *
from    pmpp.execute_command('analyze');

create table parallel_index_test( b integer, c integer, d integer );
grant all on parallel_index_test to public;

insert into parallel_index_test
select  b.b, c.c, d.d
from    generate_series(1,10) as b,
        generate_series(1,10) as c,
        generate_series(1,10) as d;

select * from parallel_index_test order by 1,2,3 limit 10;

select  b, sum(c), sum(d)
from    parallel_index_test
group by b
order by b;

select  b, sum(c), sum(d)
from    pmpp.distribute(null::parallel_index_test,
                        'dbname=' || current_database(),
                        array(  select  format('select %s, sum(c), sum(d) from parallel_index_test where b = %s',b.b,b.b)
                                from    generate_series(1,10) as b ))
group by b
order by b;


select dblink_test_schema.dblink_exec(:'loopback_su_conn_str','create index pit1 on parallel_index_test(c)');
select dblink_test_schema.dblink_exec(:'loopback_su_conn_str','drop index pit1');

select * from pmpp.execute_command('create index pit1 on parallel_index_test(c)');
select * from pmpp.execute_command('drop index pit1');


select  *
from    pmpp.meta('dbname=' || current_database(), 
                    array(  select format('create index on %s(%s)',c.table_name,c.column_name)
                            from    information_schema.columns c
                            where   c.table_name = 'parallel_index_test'
                            and     c.table_schema = 'public'))
order by 1;

\d+ parallel_index_test


----
-- test cast of array which tests cast of pmpp.query_manifest as well
select  t.*
from    unnest(:'json_str_1'::jsonb::pmpp.query_manifest[]) t;

create temporary table x(y integer);

select *
from    pmpp.distribute(null::x,:'loopback_su_conn_str',array['select 1','select 2','select 3']);

select  *
from    pmpp.manifest_set(:'json_str_1'::jsonb);

select  queries
from    pmpp.manifest_set(:'json_str_1'::jsonb);

select  *
from    pmpp.manifest_set(:'json_str_div_zero'::jsonb);

select  *
from    pmpp.manifest_set(:'json_str_timeout'::jsonb);

create temporary table r as
select  array_agg(t) r
from    pmpp.manifest_set(:'json_str_1'::jsonb) as t;

select *
from    pmpp.distribute(null::x, :'json_str_1'::jsonb);



create server :pmpp_localhost_server foreign data wrapper postgres_fdw
options (host 'localhost', dbname :'dbname' );

create user mapping for public server :pmpp_localhost_server options(user :'pmpp_test_user', password :'password');

select *
from    pmpp.distribute(null::x,:'pmpp_localhost_server',array['select 1','select 2','select 3']);

select *
from    pmpp.distribute(null::x, :'json_str_2'::jsonb);

select *
from    pmpp.distribute(null::x, :'json_str_timeout'::jsonb);

select *
from unnest(dblink_test_schema.dblink_get_connections()) as c;
select pmpp.disconnect();
select *
from unnest(dblink_test_schema.dblink_get_connections()) as c;


select *
from    pmpp.distribute(null::x, :'json_str_div_zero'::jsonb);

select *
from unnest(dblink_test_schema.dblink_get_connections()) as c;
select pmpp.disconnect();
select *
from unnest(dblink_test_schema.dblink_get_connections()) as c;

drop user mapping for public server :pmpp_localhost_server;
create user mapping for public server :pmpp_localhost_server options(user :'nopw_test_user', password '');

select *
from    pmpp.distribute(null::x, :'json_str_2'::jsonb);

drop role :pmpp_test_user;
drop role :nopw_test_user;

