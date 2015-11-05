# Range Partitioning

[![PGXN version](https://badge.fury.io/pg/range_partitioning.svg)](https://badge.fury.io/pg/range_partitioning)

Create and manage tables which will be partitioned by static ranges. The partitioning column can be any type for which there is a corresponding range type, even if that range type was user-created.

## USAGE
For function documentation and examples, see the [range_partitioning.md file](doc/range_partitioning.md).

## INSTALLATION

Requirements: PostgreSQL 9.2 or greater.

In the directory where you downloaded range_partitioning, run

```bash
make install
```

Log into PostgreSQL.

```sql
CREATE EXTENSION range_partitioning;
```

or
```sql
CREATE EXTENSION range_partitioning SCHEMA my_schema;
```

All functions created have execute granted to public.

## UPGRADE

Run "make install" same as above to put the script files and libraries in place. Then run the following in PostgreSQL itself:

```sql
ALTER EXTENSION range_partitioning UPDATE TO '<latest version>';
```

