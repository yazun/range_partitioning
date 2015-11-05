begin;
\set ECHO none
\i sql/range_partitioning.sql
\set ECHO all

create schema part_test;


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

create table part_test.bar(str text collate "C");

select exists( select null from pg_attribute where attrelid = 'part_test.bar'::regclass and attname = 'str' );

select create_parent('part_test.bar','str');
select * from p_info order by 1,2;

select create_partition('part_test.bar','(,A)');
select * from p_info order by 1,2;

select create_partition('part_test.bar','[A,C)');
select * from p_info order by 1,2;


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


rollback;

