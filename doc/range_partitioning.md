# Range Partitioning

Partition by ranges, leveraging the range types available in PostgreSQL.

## Installation

The extension is relocatable, and it is suggested that a schema be specified.

```sql
CREATE EXTENSION range_partitioning SCHEMA range_partitioning;
```

## Functions

### create_parent()

```sql
function create_parent( p_qual_table_name text,
                        p_range_column_name text,
                        p_dest_schema text default null) returns void
```

Convert a table from a normal table to the parent of a single partition.
Create the metadata entries in *master* and *partition* to reflect this change.
Create the necessary trigger such that rows inserted into the parent table actually go into the appropriate partiiton.

#### Parameters:

* **p_qual_table_name**: The qualified name of the table to be partitioned.
* **p_range_column_name**: The column in the parent table which will be the partition key.
* **p_dest_schema_name**: The name of the schema where new partitions should be created. The default is to use the schema of the parent table.

#### Example

```sql
create table part_test.foo( x integer );
select create_parent('part_test.foo','x');
 create_parent 
---------------
 
(1 row)

select  c.relname, p.partition_number, p.range
from    partition p
join    pg_class c
on      c.oid = p.partition_class
order by 1,2;
 relname | partition_number | range 
---------+------------------+-------
 foo_p0  |                0 | (,)
(1 row)
```

### create_partition()

Create a new partition by splitting it off from an existing, unspecified partition.

```sql
function create_partition (  p_qual_table_name text,
                             p_new_partition_range text) returns void 
```

#### Parameters:

* **p_qual_table_name**: The qualified name of the table to be partitioned.
* **p_new_partition_range**: The range of the new partition. This value must be a perfect subset of one of the existing partitions in the table.

#### Example

```sql

select  c.relname, p.partition_number, p.range
from    partition p
join    pg_class c
on      c.oid = p.partition_class
order by 1,2;
 relname | partition_number | range 
---------+------------------+-------
 foo_p0  |                0 | (,)
(1 row)

select create_partition('part_test.foo','[5000,)');
 create_partition 
------------------
 
(1 row)

select  c.relname, p.partition_number, p.range
from    partition p
join    pg_class c
on      c.oid = p.partition_class
order by 1,2;
 relname | partition_number |  range  
---------+------------------+---------
 foo_p0  |                0 | (,5000)
 foo_p1  |                1 | [5000,)
(2 rows)
```

### drop_partition()

Merge one partition into an existing adjacent partition.

```sql
function drop_partition (p_drop_partition_name text,
                         p_adjacent_partition_name text) returns void 
```
#### Parameters:

* **p_drop_partition_name**: The name of the partition to be dropped.
* **p_new_partition_range**: The name of a partiiton that is adjacent to p_drop_partition_name, that will annex the range of the dropped partition.

#### Example

```sql

select  c.relname, p.partition_number, p.range
from    partition p
join    pg_class c
on      c.oid = p.partition_class
order by 1,2;
 relname | partition_number |  range  
---------+------------------+---------
 foo_p0  |                0 | (,5000)
 foo_p1  |                1 | [5000,10000)
 foo_p2  |                2 | [10000,)
(3 rows)

select drop_partition('part_test.foo_p1','part_test.foo_p0');
 drop_partition 
----------------
 
(1 row)

select  c.relname, p.partition_number, p.range
from    partition p
join    pg_class c
on      c.oid = p.partition_class
order by 1,2;
 relname | partition_number |  range   
---------+------------------+----------
 foo_p0  |                0 | (,10000)
 foo_p2  |                2 | [10000,)
(2 rows)
```

### where_clause()

Given the oid of a partition, return the WHERE-clause fragment that fits the range for that partition.
The fragment will be expressed as simple < > >= <= tests, and can be blank in the case of the (,) set.

```sql
create function where_clause(p_partition_class oid) returns text
```

### Parameters

* **p_partition_class**: The oid of the partition

#### Example

```sql
select master_class::regclass::text as m, partition_class::regclass::text as p, range, where_clause(partition_class) as sql
from partition
order by 1,2;
       m       |        p         | range |           sql
---------------+------------------+-------+--------------------------
 part_test.bar | part_test.bar_p0 | [C,)  | str >= 'C'
 part_test.bar | part_test.bar_p1 | (,A)  | str < 'A'
 part_test.bar | part_test.bar_p2 | [A,C) | str >= 'A' and str < 'C'
 part_test.foo | part_test.foo_p2 | (,)   |
(4 rows)
```

## Tables

### master

Every table that is range partitioned will have an entry here.

```sql
create table master (
    master_class oid not null primary key,
    partition_attribute text not null,
    range_type oid not null,
    insert_trigger_function text not null
);
```

#### Columns

* **master_class**: points to the pg_class entry for the table that is partitioned
* **partition_attribute**: the name of the column on which the table is partitioned
* **range_type**: points to the range pg_type
* **nsert_trigger_function**: name of the trigger function created for this table

### partition

Every partition must have an entry in this table.

```sql
create table partition (
    partition_class oid not null primary key,
    master_class oid not null references master(master_class),
    partition_number integer not null,
    range text not null,
    unique(master_class,partition_number)
);
```

#### Columns

* **master_class**: points to the pg_class entry for the table that is partitioned
* **partition_class**: points to the pg_class entry for the partition
* **partition_number**: the number of this partition, used only to ensure unique partition names
* **range**: text representation of the range enforced by the check constraint

### Support

Submit issues to the [GitHub issue tracker](https://github.com/moat/range_partitioning/issues).

### Author

Corey Huinker, while working at [Moat](http://moat.com)

### Copyright and License

Copyright (c) 2015, Moat Inc.

Permission to use, copy, modify, and distribute this software and its documentation for any purpose, without fee, and without a written agreement is hereby granted, provided that the above copyright notice and this paragraph and the following two paragraphs appear in all copies.

IN NO EVENT SHALL MOAT INC. BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF Moat, Inc. HAS BEEN ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

MOAT INC. SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS IS" BASIS, AND Moat, Inc. HAS NO OBLIGATIONS TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.




