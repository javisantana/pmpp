# Poor Man's Parallel Processing (PMPP)

[![PGXN version](https://badge.fury.io/pg/pmpp.svg)](https://badge.fury.io/pg/pmpp)

PMPP is an extension that provides a means of manually dividing one large query into several smaller ones that return the same shape of result set, allowing for additional query layers on the combined set. This is accomplished through use of polymorphic functions performing asynchronous dblink commands and queue management.

## USAGE
For function documentation and examples, see the [pmpp.md file](doc/pmpp.md).

## INSTALLATION

Requirements: PostgreSQL 9.4 or greater, [dblink](http://www.postgresql.org/docs/current/static/dblink.html) extension installed.

Recommended: [postgresql_fdw](http://www.postgresql.org/docs/current/static/postgres-fdw.html). Having foreign server definitions and user mappings makes for cleaner function invocations.

In the directory where you downloaded pmpp, run

```bash
make install
```

Log into PostgreSQL.

If `dblink` is not already installed, run the following command:

```sql
CREATE EXTENSION dblink;
```

The `dblink` module does not need to be installed in any specific schema.

Run the following command:

```sql
CREATE EXTENSION pmpp;
```

This will create a role (if it does not already exist) named pmpp. To allow user to execute pmpp functions, grant them the role pmpp, like this:

```sql
GRANT pmpp to <role>;
```

PMPP makes extensive use of features introduced in PostgreSQL 9.4.0 (JSONB, WITH ORDINALITY, etc).


## UPGRADE

Run "make install" same as above to put the script files and libraries in place. Then run the following in PostgreSQL itself:

```sql
ALTER EXTENSION pmpp UPDATE TO '<latest version>';
```
