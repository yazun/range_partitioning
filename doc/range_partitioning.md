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
function where_clause(p_col text, p_range text, p_range_type oid) returns text;
function where_clause(p_col text, p_range text, p_range_type text) returns text;
```

Generate the syntax for a where clause which matches the parameters of a given range and type.

#### Parameters
* **p_col** the name of the column to use in the where clause. It does not have to be the actual column of any table, and can be a compound name.
* **p_range** the text representation of the range value.
* **p_range_type** the name of the range type, or the oid referencing a range type.

#### Example

```sql
select  where_clause('x','(,)','int4range') as w1,
        where_clause('d','(,2015-01-01)','daterange') as w2,
        where_clause('y','[4,5]','int4range') as w3,
        where_clause('z','[4,5)','int4range') as w4,
        where_clause('z','empty','int4range') as w5;
  w1  |        w2        |          w3          |          w4          |  w5   
------+------------------+----------------------+----------------------+-------
 true | d < '01-01-2015' | y >= '4' and y < '6' | z >= '4' and z < '5' | false
(1 row)
```

```sql
function where_clause(p_partition_class oid) returns text
```

This version of the function derives the column name, range, and range type from an existing partition entry.

#### Parameters

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

### value_in_range()

Determine if p_value would fit (x <@ y) into a the range p_range of type p_range_type.

```sql
create or replace function value_in_range(p_value text, p_range text, p_range_type text) returns boolean
```

#### Parameters

* **p_value**: The text representation of the value to be tested. It will be cast to the appropriate subtype of p_range_type.
* **p_range**: The text representation of the range to be tested. It will be cast to the range p_range_type.
* **p_range_type**: The name of the range type.

#### Example
```sql
select value_in_range('5','[1,10]','int4range');
 value_in_range 
----------------
 t
(1 row)

select value_in_range('11','[1,10]','int4range');
 value_in_range 
----------------
 f
(1 row)

select value_in_range('abc','[a,c]','range_partitioning_textrange_c');
 value_in_range 
----------------
 t
(1 row)

select value_in_range('efg','[a,c]','range_partitioning_textrange_c');
 value_in_range 
----------------
 f
(1 row)
```

### is_subrange()

Determine if p_little_range is a valid subrange of p_big_range.

```sql
create or replace function is_subrange(p_little_range text, p_big_range text, p_range_type text) returns boolean
```

#### Parameters

* **p_little_range**: The text representaiton of the smaller range. It will be cast to p_range_type.
* **p_big_range**: The text representaiton of the larger range. It will be cast to p_range_type.
* **p_range_type**: The name of the range type.

#### Example
```sql
select is_subrange('[4,5]','[1,10)','int4range');
 is_subrange 
-------------
 t
(1 row)

select is_subrange('[4,5]','[7,10)','int4range');
 is_subrange 
-------------
 f
(1 row)

select is_subrange('[4,7]','[5,10)','int4range');
 is_subrange 
-------------
 f
(1 row)

select is_subrange('[abc,def]','[a,e)','range_partitioning_textrange_c');
 is_subrange 
-------------
 t
(1 row)

select is_subrange('[abc,xyz]','[a,e)','range_partitioning_textrange_c');
 is_subrange 
-------------
 f
(1 row)

select is_subrange('[abc,def]','[b,z)','range_partitioning_textrange_c');
 is_subrange 
-------------
 f
(1 row)
```

### range_add()

Add two ranges together and return the text representation of the result.

```sql
create function range_add(p_range_x text, p_range_y text, p_range_type text) return text
```

#### Parameters

* **p_range_x**: the first range. It will be cast to p_range_type.
* **p_range_y**: the second range. It will be cast to p_range_type.
* **p_range_type**: The name of the range type.

#### Example
```sql
select range_add('[4,5]','(5,10]','int4range');
 range_add 
-----------
 [4,11)
(1 row)

select range_add('[4,5]','[5,10]','int4range');
 range_add 
-----------
 [4,11)
(1 row)

select range_add('[4,5]','[7,10]','int4range');
ERROR:  result of range union would not be contiguous
CONTEXT:  SQL statement "select $1::int4range + $2::int4range"
PL/pgSQL function range_add(text,text,text) line 5 at EXECUTE

select range_add('[abc,def]','(def,xyz]','range_partitioning_textrange_c');
 range_add 
-----------
 [abc,xyz]
(1 row)

select range_add('[abc,def]','[def,xyz]','range_partitioning_textrange_c');
 range_add 
-----------
 [abc,xyz]
(1 row)

select range_add('[abc,def]','[ijk,xyz]','range_partitioning_textrange_c');
ERROR:  result of range union would not be contiguous
CONTEXT:  SQL statement "select $1::range_partitioning_textrange_c + $2::range_partitioning_textrange_c"
PL/pgSQL function range_add(text,text,text) line 5 at EXECUTE
```
### range_subtract()

Subtract range y from range x together and return the text representation of the result.

```sql
create function range_subtract(p_range_x text, p_range_y text, p_range_type text) returns text
```

#### Parameters

* **p_range_x**: the larger of the two ranges. It will be cast to p_range_type.
* **p_range_y**: the smaller of the two ranges. It will be cast to p_range_type.
* **p_range_type**: The name of the range type.

#### Example

```sql
select range_subtract('[1,10]','(5,10]','int4range');
 range_subtract 
----------------
 [1,6)
(1 row)

select range_subtract('[1,10]','[1,5]','int4range');
 range_subtract 
----------------
 [6,11)
(1 row)

select range_subtract('[1,5]','[3,10]','int4range');
 range_subtract 
----------------
 [1,3)
(1 row)

select range_subtract('[abc,xyz]','[abc,def]','range_partitioning_textrange_c');
 range_subtract 
----------------
 (def,xyz]
(1 row)

select range_subtract('[abc,xyz]','[ijk,xyz]','range_partitioning_textrange_c');
 range_subtract 
----------------
 [abc,ijk)
(1 row)

select range_subtract('[def,deg]','[abc,xyz]','range_partitioning_textrange_c');
 range_subtract 
----------------
 empty
(1 row)
```

### constructor_clause()

Construct a range_type(low,high,bounds) clause for dynamic sql.

```sql
create function constructor_clause(text,text,text,text) returns text
```

#### Parameters

* **p_low**: the text representation of the lower bound (if there is one), null if no lower bound.
* **p_high**: the text representation of the upper bound (if there is one), null if no upper bound.
* **p_bounds**: standard range bound notation: (), (], [), or [].  In the case where an inclusive bound is given for a null value, the bound will be changed to infinite.

#### Example

```sql
select constructor_clause('1','5','[]','int4range');
   constructor_clause    
-------------------------
 int4range('1','5','[]')
(1 row)

select constructor_clause('ab,c','def','[]','range_partitioning_textrange_c');
                constructor_clause                 
---------------------------------------------------
 range_partitioning_textrange_c('ab,c','def','[]')
(1 row)
```

### get_destination_partition()

Get the name of the partition that can contain p_value for p_master_table.

```sql
create function get_destination_partition(p_master_table text, p_value text) returns text
```

#### Parameters

* **p_master_table**: name of the table which contains partitions.
* **p_value**: text representation of the value that should fit in one of the partitions. It will be casted to the subtype of the range partitioning type.

#### Example

```sql
select get_destination_partition('part_test.foo','4998');
 get_destination_partition 
---------------------------
 part_test.foo_p0
(1 row)

select get_destination_partition('part_test.foo','5000');
 get_destination_partition 
---------------------------
 part_test.foo_p1
(1 row)

select get_destination_partition('part_test.bar','ABEL');
 get_destination_partition 
---------------------------
 part_test.bar_p2
(1 row)

select get_destination_partition('part_test.bar','CHARLIE');
 get_destination_partition 
---------------------------
 part_test.bar_p0
(1 row)
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

## Views

### master_partition

This contains:
* all the columns in master,
* all the columns in partition that do not overlap master,
* the collation_name for the base type, if any.

### Support

Submit issues to the [GitHub issue tracker](https://github.com/moat/range_partitioning/issues).

### Author

Corey Huinker, while working at [Moat](http://moat.com)

### Copyright and License

Copyright (c) 2015, Moat Inc.

Permission to use, copy, modify, and distribute this software and its documentation for any purpose, without fee, and without a written agreement is hereby granted, provided that the above copyright notice and this paragraph and the following two paragraphs appear in all copies.

IN NO EVENT SHALL MOAT INC. BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF Moat, Inc. HAS BEEN ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

MOAT INC. SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS IS" BASIS, AND Moat, Inc. HAS NO OBLIGATIONS TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.

