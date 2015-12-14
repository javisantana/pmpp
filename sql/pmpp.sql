set client_min_messages = warning;

create temporary table uname_command_output(uname text);
create temporary table cpu_command_output(num_cpus integer);

do $$
declare
    l_uname text;
    l_num_cpus integer;
begin
    copy uname_command_output from program 'uname';
    select uname into l_uname from uname_command_output;
    if l_uname = 'Linux' then
        copy cpu_command_output from program 'nproc';
    elsif l_uname = 'Darwin' then
        copy cpu_command_output from program 'sysctl hw.ncpu | awk ''{print $2}''';
    else
        raise exception 'Unkown uname result: %', l_uname;
    end if;
    -- Windows: ???
    select num_cpus into l_num_cpus from cpu_command_output;
    execute format('create function num_cpus() returns integer language sql immutable as %L',
                    format('select %s;',l_num_cpus));
end;
$$;

drop table uname_command_output;
drop table cpu_command_output;

comment on function num_cpus()
is 'The number of cpus detected on this database machine';

create function num_cpus(multiplier in float) returns integer
language sql immutable set search_path from current as $$
select greatest(1,(multiplier * num_cpus()::float)::integer);
$$;

comment on function num_cpus(multiplier float)
is E'The number of cpus on this machine times a given multiplier.\n'
    'Never return a number below 1.';

create function num_cpus_remote(connection_string text, cpu_multiplier float default 1.0) returns integer
language sql security definer set search_path from current as $$
select  r.num_cpus
from    dblink(connection_string,format('select %I.num_cpus(%s)','@extschema@',cpu_multiplier)) as r(num_cpus integer); 
$$;

comment on function num_cpus_remote(connection_string text, cpu_multiplier float) 
is  'Get the number of cpus from a remote machine, assuming the machine has pmpp intalled';

create function disconnect() returns setof text language sql security definer
set search_path from current as $$
select  dblink_cancel_query(r.s)
from    unnest(dblink_get_connections()) as r(s)
where   r.s like '@extschema@.%'
and     dblink_is_busy(r.s) = 1;
select  dblink_disconnect(r.s)
from    unnest(dblink_get_connections()) as r(s)
where   r.s like '@extschema@.%';
$$;

comment on function disconnect()
is  'Disconnect from all @extschema@.* connections';

create function jsonb_array_to_text_array(jsonb_arr jsonb) returns text[]
language sql as $$
select  array_agg(r)
from    jsonb_array_elements_text(jsonb_arr) r;
$$;

comment on function jsonb_array_to_text_array(jsonb jsonb)
is E'Function to bridge the gap between json arrays and pgsql arrays.\n'
    'This will probably go away when native postgres implements this.';

create type query_manifest as (
    connection text,
    queries text[],
    cpu_multiplier float,
    num_workers integer,
    statement_timeout integer
);

comment on type query_manifest
is 'The basic data structure for nesting commands for distribution';

create function to_query_manifest(item in jsonb) returns query_manifest
language sql strict immutable set search_path from current as $$
select  item->>'connection' as connection,
        -- coalesce the 'queries' array with the 'query' scalar and then remove any nulls.
        -- They shouldn't both appear but if they did there is no reason not to play nice.
        array_remove(jsonb_array_to_text_array(item->'queries') || (item->>'query'),null) as queries,
        (item->>'cpu_multiplier')::float as cpu_multiplier,
        (item->>'num_workers')::integer as num_workers,
        (item->>'statement_timeout')::integer as statement_timeout;
$$;

comment on function to_query_manifest(item in jsonb)
is  'Convert a JSONB manifest item to a query_manifest';

create cast (jsonb as query_manifest) with function to_query_manifest(jsonb) as implicit;

comment on cast (jsonb as query_manifest)
is  'Cast JSONB->query_manifest';

create function to_query_manifest_array(manifest in jsonb) returns query_manifest[]
language sql strict immutable set search_path from current as $$
select  array_agg(q::query_manifest)
from    jsonb_array_elements(manifest) q;
$$;

comment on function to_query_manifest_array(item in jsonb)
is  'Convert a JSONB manifest item array to a query_manifest[]';

create cast (jsonb as query_manifest[]) with function to_query_manifest_array(jsonb) as implicit;

comment on cast (jsonb as query_manifest[])
is  'Cast JSONB->query_manifest[]';

create function manifest_set( query_manifest jsonb ) returns setof query_manifest
language sql set search_path from current as $$
select  q::query_manifest
from    jsonb_array_elements(query_manifest) q;
$$;

comment on function manifest_set( query_manifest jsonb )
is E'Decompose a query manifest jsonb into the proper pieces.\n'
    'This is exposed largely for debugging purposes.';

create function manifest_array( query_manifest jsonb ) returns query_manifest[]
language sql set search_path from current as $$
select  array_agg(t)
from    manifest_set(query_manifest) as t;
$$;

comment on function manifest_array( query_manifest jsonb )
is E'Decompose a query manifest jsonb into an array suitable for distribute().\n'
    'This is exposed largely for debugging purposes.';

create function distribute( row_type anyelement,
                            query_manifest query_manifest[] )
                            returns setof anyelement
language plpgsql
security definer
set search_path from current as $$
declare
    r record;
    fetch_results_query text;
begin
    -- decompose the json into a temporary table
    create temporary table pmpp_manifest
    as
    select  q.connection, 
            q.ordinality as server_id,
            q.num_workers,
            coalesce(q.cpu_multiplier,1.0) as cpu_multiplier,
            q.statement_timeout,
            q.queries,
            array_length(q.queries,1) as num_queries
    from    unnest(query_manifest) with ordinality as q
    where   array_length(q.queries,1) > 0;
            
    -- create a table of all queries to be run
    create temporary table pmpp_jobs
    as
    select  m.server_id,
            s.id_within_server,
            s.query_string
    from    pmpp_manifest m,
            lateral unnest(m.queries) with ordinality as s(query_string,id_within_server);

    -- create a table of workers, one per available slot on each connection, 
    -- and assign the first query to each of those
    create temporary table pmpp_workers
    as
    with workers_per_connection as (
        -- if there is only one query, only create one worker.
        select  m.server_id,
                m.connection,
                1 as id_within_server
        from    pmpp_manifest m
        where   m.num_queries = 1
        union all
        -- if num_workers is specified, then don't ask the remote how many cpus it has
        -- some backends may not have pmpp installed, or may not even support functions (redshift, etc)
        -- or we just might already know the answer and want to save a connection
        select  m.server_id,
                m.connection,
                s.id_within_server
        from    pmpp_manifest m,
                lateral generate_series(1,least(m.num_workers,m.num_queries)) as s(id_within_server)
        where   m.num_workers is not null
        and     m.num_queries > 1
        union all
        -- for everyone else, we ask the remote how many CPUs it has
        select  m.server_id,
                m.connection,
                s.id_within_server
        from    pmpp_manifest m,
                lateral generate_series(1,least(num_cpus_remote(m.connection,m.cpu_multiplier),
                                                m.num_queries)) as s(id_within_server)
        where   m.num_workers is null
        and     m.num_queries > 1
    )
    -- create one row per worker, and assign it a job
    select  wpc.server_id,
            wpc.id_within_server,
            format('%s.%s','@extschema@',row_number() over ()) as dblink_connname,
            wpc.connection,
            j.query_string
    from    workers_per_connection wpc
    join    pmpp_jobs j
    on      j.server_id = wpc.server_id
    and     j.id_within_server = wpc.id_within_server;

    -- delete the jobs we just assigned
    delete
    from    pmpp_jobs
    where   (server_id,id_within_server) in (select w.server_id, w.id_within_server from pmpp_workers w);

    -- drop any existing pmpp connections
    perform disconnect();

    -- make all dblink connections and statement timeouts one at a time, as each can throw an exception
    for r in (  select  w.dblink_connname,
                        w.connection,
                        w.query_string,
                        m.statement_timeout,
                        format('set statement_timeout = %s',m.statement_timeout) as set_sql
                from    pmpp_workers w
                join    pmpp_manifest m
                on      m.server_id = w.server_id   )
    loop
        begin
            perform dblink_connect_u(r.dblink_connname,r.connection);
        exception
            when others then
                raise warning 'error connecting %/%', r.dblink_connname, r.connection;
                raise;
        end;
        -- set statement timeouts where required 
        if r.statement_timeout is not null then
            begin
                perform dblink_exec(r.dblink_connname,r.set_sql);
            exception
                when others then
                    raise warning 'connection %/% query: %', r.dblink_connname, r.connection, r.set_sql;
                    raise;
            end;
        end if;

        -- does this clear off spurious error messages
        perform dblink_get_result(r.dblink_connname);

        if dblink_send_query(r.dblink_connname,r.query_string) <> 1 then
            raise warning 'connection %/% query %', r.dblink_connname, r.connection, r.query_string;
            raise exception '%', dblink_error_message(r.dblink_connname);
        end if;
    end loop;

    -- use this time after the initial queries were launched to clean up old structures and get ready to
    -- retrieve results.

    -- not needed anymore
    drop table pmpp_manifest;

    -- figure out the column list of this composite type, ignoring system columns. This query takes about 1ms
    with x as (
        select  a.attname || ' ' || pg_catalog.format_type(a.atttypid, a.atttypmod) as sql_text
        from    pg_catalog.pg_attribute a
        where   a.attrelid = pg_typeof(row_type)::text::regclass
        and     a.attisdropped is false
        and     a.attnum > 0
        order by a.attnum )
    select  format('select * from dblink_get_result($1) as t(%s)',string_agg(x.sql_text,','))
    into    fetch_results_query
    from    x;

    -- as long as we still have workers, loop to see if they're done with what they're doing
    while exists(select null from pmpp_workers)
    loop
        if not exists (select null from pmpp_jobs) then
            -- if there are no more jobs to distribute,
            if (select count(*) from (select null from pmpp_workers limit 2) as x) = 1 then
                -- and there is only one worker left,
                -- then don't poll for completion, just do a blocking fetch on that last connection
                delete from pmpp_workers returning * into r;
                begin
                    return query execute fetch_results_query using r.dblink_connname;
                exception
                    when query_canceled or others then
                        perform dblink_disconnect(r.dblink_connname);
                        raise warning 'connection %/% query: %', r.dblink_connname, r.connection, r.query_string;
                        raise;
                end;
                -- and then disconnect
                perform dblink_disconnect(r.dblink_connname);
                exit;
            end if;
        end if;
        -- sleep until at least one worker has completed a query
        while not exists(select null from pmpp_workers w where dblink_is_busy(w.dblink_connname) = 0)
        loop
            -- sleep just enough to give up the timeslice, giving connections time to finish their queries
            perform pg_sleep(0.01);
        end loop;
        -- find all of the jobs that are currently finished and need fetching
        for r in (  select  w.server_id, w.dblink_connname, w.connection, w.query_string
                    from    pmpp_workers w
                    where   dblink_is_busy(w.dblink_connname) = 0)
        loop
            -- fetch the real result
            begin
                return query execute fetch_results_query using r.dblink_connname;
            exception
                when query_canceled or others then
                    perform dblink_disconnect(r.dblink_connname);
                    raise warning 'connection %/% query: %', r.dblink_connname, r.connection, r.query_string;
                    raise;
            end;

            -- pick another query destined for that worker's server, any one will do
            delete
            from    pmpp_jobs
            where   ctid = (select ctid from pmpp_jobs where server_id = r.server_id limit 1)
            returning query_string
            into r.query_string;

            if found then
                -- set the new current query
                update  pmpp_workers
                set     query_string = r.query_string
                where   dblink_connname = r.dblink_connname;

                -- to reset the connection, we must fetch the empty set
                perform dblink_get_result(r.dblink_connname);
                -- assign next job
                if dblink_send_query(r.dblink_connname,r.query_string) <> 1 then
                    raise warning 'connection %/% query: %', r.dblink_connname, r.connection, r.query_string;
                    raise;
                end if;
            else
                -- no work left to give, shut down the connection
                perform dblink_disconnect(r.dblink_connname);

                delete
                from    pmpp_workers w
                where   w.dblink_connname = r.dblink_connname;
            end if;
        end loop;
    end loop;
    perform disconnect();
    drop table pmpp_jobs;
    drop table pmpp_workers;
exception
    when others then
        perform disconnect();
        drop table if exists pmpp_jobs;
        drop table if exists pmpp_workers;
        drop table if exists pmpp_manifest;
        raise;
end;
$$;

comment on function distribute(anyelement,query_manifest[])
is E'For each query manifest object in the array:\n'
    'Connect to each of the databases with a number of connections equal to num_workers (if specified), or \n'
    'the number of cpus found on the machine at the end of that connection times the mulitplier (defaults to 1.0).\n'
    'Each query specified must return a result set matching the type row_type.\n'
    'The queries are distributed to the connections as they become available, and the results of those queries are\n'
    'in turn pushed to the result set, where they can be further manipulated like any table, view, or set returning function.\n'
    'Returns a result set in the shape of row_type.';

create function distribute( row_type anyelement,
                            query_manifest jsonb )
                            returns setof anyelement
language sql set search_path from current as $$
select distribute(row_type,query_manifest::query_manifest[])
$$;

comment on function distribute(anyelement,jsonb)
is E'Alternate version of distribute that takes a jsonb document instead of an array of query_manifest.\n'
    'The JSON document must be an array of objects in the the following structure:\n'
    '\t [{"connection": "connection-string", "queries": ["query 1","query 2", ...], "num_workers": 4},\n'
    '\t  {"connection": "connection-string", "queries": ["query 10", "query 11", ...], "multiplier": 1.5},\n'
    '\t [{"connection": "connection-string", "query": "query sql"},\n'
    '\t  ...]\n'
    'Other elements within the objects would be ignored.';



create function distribute( row_type anyelement,
                            query_manifest in json )
                            returns setof anyelement
language sql security definer set search_path from current
as $$
select distribute(row_type,query_manifest::jsonb);
$$;

comment on function distribute(anyelement,json)
is 'Pass-through function so that we can accept json as well as jsonb';

create function distribute( row_type anyelement,
                            connection text,
                            sql_list text[],
                            cpu_multiplier float default 1.0,
                            num_workers integer default null,
                            statement_timeout integer default null)
                            returns setof anyelement
language sql security definer set search_path from current 
as $$
select  distribute(row_type,
                    array[ row(connection,sql_list,cpu_multiplier,num_workers,statement_timeout)::query_manifest ]);
$$;

comment on function distribute(anyelement,text,text[],float,integer,integer)
is E'Given an array of sql commands, execute each one against an async conneciton specified,\n'
    'spawning as many connections as specified by the multiplier of the number of cpus on the machine,\n'
    'returning a set of data specified by the row type.\n'
    'This simpler form is geared towards basic parallelism rather than distributed queries';

do $$
declare
    l_role_exists boolean;
begin
    select exists( select null from pg_roles where rolname = 'pmpp') into l_role_exists;
    if not l_role_exists then
        execute 'create role pmpp';
    end if;
end
$$;

create type command_result as ( result text );
comment on type command_result
is 'Simple type for getting the results of non-set-returning-function commands';

create type command_with_result as ( command text, result text );

comment on type command_with_result
is E'Simple type for getting the results of non-set-returning-function commands\n'
    'and the SQL that generated that result';


-- search path and security must match the connection, not the definer
create function execute_command(sql text) returns command_with_result
language plpgsql as $$
begin
    -- I would rather we returned the string from PQcmdStatus(PGresult *res), but for now OK/FAIL will have to do
    execute sql;
    return (sql,'OK')::@extschema@.command_with_result;
exception when others then
    return (sql,'FAIL')::@extschema@.command_with_result;
end
$$;

comment on function execute_command(sql text)
is 'Execute a non-set-returning-function and report the success or failure of it';

create function meta(   connection text,
                        sql_list text[],
                        cpu_multiplier float default null,
                        num_workers integer default null,
                        statement_timeout integer default null) returns setof command_with_result
language sql security definer set search_path from current as $$
select  *
from    distribute( null::command_with_result,
                    connection,
                    array(  select  format('select * from "@extschema@".execute_command(%L)',t.sql)
                            from    unnest(sql_list) as t(sql) ),
                    cpu_multiplier,
                    num_workers,
                    statement_timeout);
$$;

comment on function meta(text,text[],float,integer,integer)
is E'Convenience routine for executing non-SELECT statements in parallel.\n'
    'Example:\n'
    'SELECT *\n'
    'FROM pmpp.meta(''loopback'', array( SELECT ''CREATE INDEX ON '' || c.table_name || ''('' || c.column_name || '')''\n'
    '                                    FROM information_schema.columns \n'
    '                                    WHERE table_name = ''''my_table_name'''' and table_schema = ''my_schema''))';


create function broadcast(  row_type anyelement,
                            connections text[],
                            query text) returns setof anyelement
language sql
security definer
set search_path from current as $$
select  distribute(row_type,
                    array(  select  row(c.conn, array[query], null, 1, null)::query_manifest
                            from    unnest(connections) c(conn)));
$$;

comment on function broadcast(anyelement,text[],text)
is E'Execute a single SQL query across a list of connections\n'
    'Example:\n'
    'CREATE TEMPORARY TABLE row_counts( rowcount bigint );\n'
    'SELECT *\n'
    'FROM pmpp.broadcast(null::row_counts,\n'
    '                    array( select srvname from pg_foreign_server ),\n'
    '                    ''selct count(*) from pg_class'' );';

grant usage on schema @extschema@ to pmpp;
grant execute on all functions in schema @extschema@ to pmpp;


