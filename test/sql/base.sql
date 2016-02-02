\set ECHO queries
create extension range_partitioning;

create schema part_test;

select value_in_range('5','[1,10]','int4range');
select value_in_range('11','[1,10]','int4range');

select is_subrange('[4,5]','[1,10)','int4range');
select is_subrange('[4,5]','[7,10)','int4range');
select is_subrange('[4,7]','[5,10)','int4range');

select range_add('[4,5]','(5,10]','int4range');
select range_add('[4,5]','[5,10]','int4range');
select range_add('[4,5]','[7,10]','int4range');

select range_subtract('[1,10]','(5,10]','int4range');
select range_subtract('[1,10]','[1,5]','int4range');
select range_subtract('[1,5]','[3,10]','int4range');

select constructor_clause('1','5','[]','int4range');

select  where_clause('x','(,)','int4range') as w1,
        where_clause('d','(,2015-01-01)','daterange') as w2,
        where_clause('y','[4,5]','int4range') as w3,
        where_clause('z','[4,5)','int4range') as w4,
        where_clause('z','empty','int4range') as w5;


create view p_info
as
select  c.relname, p.partition_number, p.range
from    partition p
join    pg_class c
on      c.oid = p.partition_class;

create table part_test.foo( x integer );

select create_parent('part_test.foo','x');
select * from p_info order by 1,2;

select  mt.relname as table_name,
        pt.relname as partition_name,
        cons.consrc
from    pg_namespace vs
join    pg_class mt
on      vs.oid = mt.relnamespace
        and
        mt.relkind = 'r'
join    pg_inherits i
on      i.inhparent = mt.oid
join    pg_class pt
on      pt.oid = i.inhrelid
        and
        pt.relkind = 'r'
join    pg_constraint cons
on      cons.conrelid = pt.oid
        and
        cons.contype = 'c'
where   vs.nspname = 'part_test'
and     mt.relname = 'foo'
order by 1,2,3;

select create_partition('part_test.foo','[5000,)');
select * from p_info order by 1,2;

select exists( select null from pg_class where relname = 'foo_p0');

select exists( select null from pg_class where relname = 'foo_p1');

insert into part_test.foo(x)
values (10),(4999),(5000),(15000);

select * from part_test.foo_p0 order by 1;

select * from part_test.foo_p1 order by 1;

select create_partition('part_test.foo','[10000,)');
select * from p_info order by 1,2;

select * from part_test.foo_p0 order by 1;

select * from part_test.foo_p1 order by 1;

select * from part_test.foo_p2 order by 1;

select get_destination_partition('part_test.foo','4998');
select get_destination_partition('part_test.foo','5000');

select master_class::regclass::text as m, partition_class::regclass::text as p, range, where_clause(partition_class) as sql
from partition
order by 1,2;

select drop_partition('part_test.foo_p1','part_test.foo_p0');
select * from p_info order by 1,2;

select * from part_test.foo_p0 order by 1;

select * from part_test.foo_p2 order by 1;

select drop_partition('part_test.foo_p0','part_test.foo_p2');
select * from p_info order by 1,2;

select * from part_test.foo_p2 order by 1;

create type range_partitioning_textrange_c as range (subtype = text, collation = "C");

select value_in_range('abc','[a,c]','range_partitioning_textrange_c');
select value_in_range('efg','[a,c]','range_partitioning_textrange_c');

select is_subrange('[abc,def]','[a,e)','range_partitioning_textrange_c');
select is_subrange('[abc,xyz]','[a,e)','range_partitioning_textrange_c');
select is_subrange('[abc,def]','[b,z)','range_partitioning_textrange_c');

select range_add('[abc,def]','(def,xyz]','range_partitioning_textrange_c');
select range_add('[abc,def]','[def,xyz]','range_partitioning_textrange_c');
select range_add('[abc,def]','[ijk,xyz]','range_partitioning_textrange_c');

select range_subtract('[abc,xyz]','[abc,def]','range_partitioning_textrange_c');
select range_subtract('[abc,xyz]','[ijk,xyz]','range_partitioning_textrange_c');
select range_subtract('[def,deg]','[abc,xyz]','range_partitioning_textrange_c');

select constructor_clause('ab,c','def','[]','range_partitioning_textrange_c');

create table part_test.bar(str text collate "C");

select exists( select null from pg_attribute where attrelid = 'part_test.bar'::regclass and attname = 'str' );

select create_parent('part_test.bar','str');
select * from p_info order by 1,2;

select create_partition('part_test.bar','(,A)');
select * from p_info order by 1,2;

select create_partition('part_test.bar','[A,C)');
select * from p_info order by 1,2;

select get_destination_partition('part_test.bar','ABEL');
select get_destination_partition('part_test.bar','CHARLIE');

insert into part_test.bar
values ('ABEL'),('BAKER'),('CHARLIE');

select * from part_test.bar_p0 order by 1;

select * from part_test.bar_p1 order by 1;

select * from part_test.bar_p2 order by 1;

select master_class::regclass::text as m, partition_class::regclass::text as p, range, where_clause(partition_class) as sql
from partition
order by 1,2;

select drop_partition('part_test.bar_p1','part_test.bar_p2');
select * from p_info order by 1,2;

select * from part_test.bar_p0 order by 1;

select * from part_test.bar_p2 order by 1;

set search_path = public;

select partition_class::regclass::text, refresh_exclusion_constraint(partition_class) from partition order by 1;

create type duplicate_int_range as range (subtype = integer );

create table dupe_test( x integer);

select create_parent('dupe_test','x');

select create_parent('dupe_test','x', p_qual_range_type := 'int4range');

create table dupe_test2( x integer);

select create_parent('dupe_test2','x', p_qual_range_type := 'duplicate_int_range');



