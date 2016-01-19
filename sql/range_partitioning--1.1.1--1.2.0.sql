
create view master_partition
as
select  m.*,
        p.partition_class,
        p.partition_number,
        p.range,
        l.collname::text as collation_name
from    master m
join    partition p
on      p.master_class = m.master_class
join    pg_range r
on      r.rngtypid = m.range_type
left
join    pg_collation l
on      l.oid = r.rngcollation;

comment on view master_partition
is 'view to join all relevant partition information';

create or replace function value_in_range(p_value text, p_range text, p_range_type text) returns boolean
language plpgsql set search_path from current as $$
declare
    l_result boolean;
begin
    execute format('select $1::%s <@ $2::%s',
                    (   select  format_type(rngsubtype,null)
                        from    pg_range
                        where   rngtypid = p_range_type::regtype ),
                    p_range_type::regtype::text)
    using p_value, p_range
    into l_result;
    return l_result;
end
$$;

comment on function value_in_range(text,text,text)
is 'determine if p_value would fit into a the range p_range of type p_range_type';

create or replace function is_subrange(p_little_range text, p_big_range text, p_range_type text) returns boolean
language plpgsql set search_path from current as $$
declare
    l_result boolean;
begin
    execute format('select $1::%1$I <@ $2::%1$I',p_range_type)
    into    l_result
    using   p_little_range, p_big_range;
    return l_result;
end
$$;

comment on function is_subrange(text,text,text)
is 'determine if p_little_range is a valid subrange of p_big_range';

create or replace function range_add(p_range_x text, p_range_y text, p_range_type text) returns text
language plpgsql set search_path from current as $$
declare
    l_result text;
begin
    execute format('select $1::%1$s + $2::%1$s',p_range_type)
    using p_range_x, p_range_y
    into l_result;
    return l_result;
end
$$;

comment on function range_add(p_range_x text, p_range_y text, p_range_type text)
is 'add two ranges together and return the text representation of the result';

create or replace function range_subtract(p_range_x text, p_range_y text, p_range_type text) returns text
language plpgsql set search_path from current as $$
declare
    l_result text;
begin
    execute format('select $1::%1$s - $2::%1$s',p_range_type)
    using p_range_x, p_range_y
    into l_result;
    return l_result;
end
$$;

comment on function range_subtract(p_range_x text, p_range_y text, p_range_type text)
is 'subtract range y from range x together and return the text representation of the result';

create or replace function get_collation(p_master_class oid) returns text
language sql as $$
select  collation_name
from    master_partition
where   master_class = p_master_class;
$$;

create or replace function where_clause(p_partition_class oid) returns text
language sql set search_path from current as $$
select  where_clause(partition_attribute,range,range_type,collation_name)
from    master_partition
where   partition_class = p_partition_class;
$$;

create function constructor_clause(p_low text, p_high text, p_bounds text, p_range_type_name text) returns text
language sql set search_path from current as $$
select  format('%I(%L,%L,%L)',
                p_range_type_name,
                p_low,
                p_high,
                p_bounds);
$$;

comment on function constructor_clause(text,text,text,text)
is E'construct a range_type(low,high,bounds) clause for dynamic sql';

create function get_destination_partition(p_master_table text, p_value text) returns text
language sql set search_path from current as $$
-- Invoking a dynamic sql function for every row is slower per row, but the effect costs 3ms for a 100 partition table
-- and 200ms for a 10,000 partition table. Cleaner SQL is worth that.
select  partition_class::regclass::text
from    master_partition
where   master_class = p_master_table::regclass
and     value_in_range(p_value,range,range_type::regtype::text);
$$;

comment on function get_destination_partition(p_master_table text, p_value text)
is 'get the name of the partition that can contain p_value for p_master_table';

create or replace function create_exclusion_constraint(p_partition_class oid) returns void language plpgsql as $$
begin
    execute format('alter table %s add constraint %I check (%s)',
                    p_partition_class::regclass::text,
                    get_exclusion_constraint_name(p_partition_class),
                    where_clause(p_partition_class));
end
$$;

create or replace function create_partition (  p_qual_table_name text,
                                    p_new_partition_range text) returns void 
language plpgsql set search_path from current as $$
declare
    mr master%rowtype;
    pr partition%rowtype;
    l_new_partition text;
    l_new_partition_number integer;
    l_range_difference text;
begin
    -- verify that we actually have a partitioned table
    select  *
    into strict mr
    from    master
    where   master_class = p_qual_table_name::regclass;

    -- figure out the number of the new partition that we are about to create
    select  max(partition_number) + 1
    into    l_new_partition_number
    from    partition
    where   master_class = mr.master_class;

    begin
        -- verify new range is entirely within an existing range, and matches one edge of that range
        select  partition_class,
                range_subtract(range,p_new_partition_range,mr.range_type::regtype::text)
        into strict pr.partition_class, l_range_difference
        from    partition
        where   master_class = mr.master_class
        and     is_subrange(p_new_partition_range,range,mr.range_type::regtype::text);
    exception
        when no_data_found or data_exception then
            raise exception 'New range {%} must match have one boundary in common with an existing partition',
                            p_new_partition_range;
    end;

    if l_range_difference = 'empty' then
        raise notice 'New partition {%} exactly matches an existing partition {%}, skipping',
                        p_new_partition_range, pr.partition_class::regclass::text;
        return;
    end if;

    l_new_partition := format('%I.%I',
                                (   select n.nspname
                                    from pg_class c
                                    join pg_namespace n on n.oid = c.relnamespace
                                    where c.oid = pr.partition_class),
                                (   select c.relname || '_p' || l_new_partition_number
                                    from pg_class c
                                    where c.oid = mr.master_class ));

    -- create the table that will inherit from the master table
    execute format('create table %s(like %s including indexes)',
                    l_new_partition,
                    mr.master_class::regclass::text);

    -- inserting into partition will automatically add the check constraint on the table and complete the inherance
    insert into partition(partition_class,master_class,partition_number,range)
    values (l_new_partition::regclass, mr.master_class, l_new_partition_number, p_new_partition_range);

    -- migrate rows to main partition
    execute format('with d as (delete from %s where %I <@ %L::%s returning *) insert into %s select * from d',
                    pr.partition_class::regclass::text,
                    mr.partition_attribute,
                    p_new_partition_range,
                    mr.range_type::regtype::text,
                    l_new_partition);

    -- updating this table will drop the old constraint and create a new one
    update  partition
    set     range = l_range_difference
    where   partition_class = pr.partition_class;

    perform create_trigger_function(mr.master_class);
end;
$$;

do $$
declare r record;
begin
    for r in (  select	p.proname, pg_get_function_identity_arguments(p.oid) as args
                from	pg_proc p
                join	pg_depend d on d.objid = p.oid and d.deptype = 'e'
                join	pg_extension x on x.oid = d.refobjid
                where   x.extname = 'range_partitioning' )
    loop
        execute format('grant execute on function %s(%s) to range_partitioning',r.proname,r.args);
    end loop;
end;
$$;

