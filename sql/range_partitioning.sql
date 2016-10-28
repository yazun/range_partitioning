create table master (
    master_class oid not null primary key,
    partition_attribute text not null,
    range_type oid not null,
    insert_trigger_function text not null
);

-- configure this table to not be ignored by pg_dump
select pg_catalog.pg_extension_config_dump('master', '');

comment on table master
is E'every table that is range partitioned will have an entry here.';

comment on column master.master_class
is E'points to the pg_class entry for the table that is partitioned';

comment on column master.partition_attribute
is E'the name of the column on which the table is partitioned';

comment on column master.range_type
is E'points to the range pg_type';

comment on column master.insert_trigger_function
is E'name of the trigger function created for this table';


create table partition (
    partition_class oid not null primary key,
    master_class oid not null references master(master_class),
    partition_number integer not null,
    range text not null,
    unique(master_class,partition_number)
);

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

-- configure this table to not be ignored by pg_dump
select pg_catalog.pg_extension_config_dump('partition', '');

comment on table partition
is E'every partition must have an entry in this table';

comment on column partition.master_class
is E'points to the pg_class entry for the table that is partitioned';

comment on column partition.partition_class
is E'points to the pg_class entry for the partition';

comment on column partition.partition_number
is E'the number of this partition, used only to ensure unique partition names';

comment on column partition.range
is E'text representation of the range enforced by the check constraint';

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
    execute format('select $1::%1$s <@ $2::%1$s',p_range_type)
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


create function get_collation(p_master_class oid) returns text
language sql as $$
select  collation_name
from    master_partition
where   master_class = p_master_class;
$$;

comment on function get_collation(p_master_class oid)
is 'return the text name of the collation for this partition key, if any';

create function range_type_info(p_range text, p_range_type oid, empty out boolean,
                                lower out text, lower_inc out boolean, lower_inf out boolean,
                                upper out text, upper_inc out boolean, upper_inf out boolean)
language plpgsql set search_path from current as $$
declare
    l_sql text;
begin
    execute format( 'select  lower(x.x)::text, upper(x.x)::text, isempty(x.x), '
                    '        lower_inc(x.x), upper_inc(x.x), lower_inf(x.x), upper_inf(x.x) '
                    'from    ( select $1::%s as x ) x',
                    (   select  format_type(p_range_type,null)))
    using   p_range
    into strict lower, upper, empty, lower_inc, upper_inc, lower_inf, upper_inf;
end;
$$;

comment on function range_type_info(text, oid, out text, out boolean, out boolean, out text, out boolean, out boolean)
is E'given a text representation of a range and the name of the range type, create that range\n'
    'and then run the lower(), upper(), lower_inc(), upper_inc(), lower_inf(), and upper_inf() functions';

create function range_type_info(p_range text, p_range_type text, empty out boolean,
                                lower out text, lower_inc out boolean, lower_inf out boolean,
                                upper out text, upper_inc out boolean, upper_inf out boolean)
language sql set search_path from current as $$
select  *
from    range_type_info(p_range,p_range_type::regtype);
$$;

comment on function range_type_info(text, text, out text, out boolean, out boolean, out text, out boolean, out boolean)
is E'given a text representation of a range and the name of the range type, create that range\n'
    'and then run the lower(), upper(), lower_inc(), upper_inc(), lower_inf(), and upper_inf() functions';

create function collate_clause(p_collation text) returns text language sql strict as $$
select format(' COLLATE "%s"',p_collation);
$$;

comment on function collate_clause(p_collation text)
is 'create a COLLATE "X" clause if and only if p_collation is not null';

create function where_clause(p_col text, p_range text, p_range_type oid, p_collation text default null) returns text
language sql set search_path from current as $$
select  case
            when i.lower = i.upper then format('%I = %L',p_col,i.lower)
            when i.lower_inf and i.upper_inf then 'true'
            when i.empty then 'false'
            else    case
                        when i.lower_inf then ''
                        when i.lower_inc then format('%I%s >= %L',p_col,collate_clause(p_collation),i.lower)
                        else format('%I%s > %L',p_col,collate_clause(p_collation),i.lower)
                    end ||
                    case
                        when not i.lower_inf and not i.upper_inf then ' and ' 
                        else ''
                    end ||
                    case
                        when i.upper_inf then ''
                        when i.upper_inc then format('%I%s <= %L',p_col,collate_clause(p_collation),i.upper)
                        else format('%I%s < %L',p_col,collate_clause(p_collation),i.upper)
                    end
        end
from    range_type_info(p_range,p_range_type) i
$$;

comment on function where_clause(text,text,oid,text)
is E'construct a WHERE clause that would exactly fit the given column, range, and range_type';

create function where_clause(p_col text, p_range text, p_range_type text, p_collation text default null) returns text
language sql set search_path from current as $$
select  where_clause(p_col,p_range,p_range_type::regtype,p_collation);  
$$;

comment on function where_clause(text,text,text,text)
is E'construct a WHERE clause that would exactly fit the given column, range, and range_type';

create function where_clause(p_partition_class oid) returns text
language sql set search_path from current as $$
select  where_clause(partition_attribute,range,range_type,collation_name)
from    master_partition
where   partition_class = p_partition_class;
$$;

comment on function where_clause(oid)
is E'given a partiton oid, derive the WHERE clause that would exactly fit the range of the partition.';

create function get_exclusion_constraint_name(p_partition_class oid) returns text language sql strict as $$
select  c.relname::text
from    pg_class c
where   c.oid = p_partition_class;
$$;

comment on function get_exclusion_constraint_name(p_partition_class oid)
is 'generate the name of the exclusion constraint for this partition';

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

create function drop_exclusion_constraint(p_partition_class oid) returns void language plpgsql as $$
begin
    execute format('alter table %s drop constraint %I',
                    p_partition_class::regclass::text,
                    get_exclusion_constraint_name(p_partition_class));
end
$$;

comment on function drop_exclusion_constraint(p_partition_class oid)
is 'internal use only';


create function create_exclusion_constraint(p_partition_class oid) returns void language plpgsql as $$
begin
    execute format('alter table %s add constraint %I check (%s)',
                    p_partition_class::regclass::text,
                    get_exclusion_constraint_name(p_partition_class),
                    where_clause(p_partition_class));
end
$$;

comment on function create_exclusion_constraint(p_partition_class oid)
is 'create the exclusion constraint for a given partition';

create function refresh_exclusion_constraint(p_partition_class oid) returns boolean
language plpgsql set search_path from current as $$
begin
    perform drop_exclusion_constraint(p_partition_class);
    perform create_exclusion_constraint(p_partition_class);
    return true;
end
$$;

comment on function refresh_exclusion_constraint(p_partition_class oid)
is 'drop and recreate exclusion constraints';

create function partition_copy_acl() returns trigger
language plpgsql security definer set search_path from current as $$
begin
    if TG_OP = 'INSERT' then
        -- copy permissions from master
        update  pg_class
        set     relacl = ( select relacl from pg_class where oid = new.master_class )
        where   oid = new.partition_class;

        -- set ownership
        execute format('alter table %I.%I owner to %I',
                        (   select  s.nspname
                            from    pg_class c
                            join    pg_namespace s
                            on      s.oid = c.relnamespace
                            where   c.oid = new.master_class ),
                        (   select  c.relname
                            from    pg_class c
                            where   c.oid = new.master_class ),
                        (   select  a.rolname
                            from    pg_class c
                            join    pg_authid a
                            on      a.oid = c.relowner
                            where   c.oid = new.master_class ) );
    end if;
    -- after trigger, no need to return anything special
    return null;
end
$$;

comment on function partition_copy_acl()
is E'This is security definer because it references pg_authid and updates pg_class directly';

create function partition_reflect() returns trigger
language plpgsql set search_path from current as $$
begin
    if TG_OP = 'UPDATE' then
        if new.master_class <> old.master_class then
            raise exception '%', 'Cannot modify master_class';
        elsif new.partition_number <> old.partition_number then
            raise exception '%', 'Cannot modify partition_number';
        elsif new.partition_class <> old.partition_class then
            raise exception '%', 'Cannot modify partition_class';
        elsif new.range is distinct from old.range then
            perform drop_exclusion_constraint(new.partition_class);
            perform create_exclusion_constraint(new.partition_class);
        end if;
    end if;

    if TG_OP = 'INSERT' then
        -- complete the inheritance
        execute format('alter table %s inherit %s',
                        new.partition_class::regclass::text,
                        new.master_class::regclass::text);
        perform create_exclusion_constraint(new.partition_class);
    end if;

    -- update the insert trigger to reflect the new partition table
    execute format(E'create or replace function %s() returns trigger language plpgsql as $BODY$\n'
                    'begin\n%sreturn null;\nend;$BODY$',
                    ( select insert_trigger_function from master where master_class = new.master_class ),
                    trigger_iter(new.master_class));

    -- after trigger, no need to return anything
    return null;
end
$$;


comment on function partition_reflect() 
is E'Reflect whatever changes were made to the partition table in the actual metadata(constraints, inheritance) of the table.';

create trigger partition_reflect after insert or update on partition for each row execute procedure partition_reflect();
create trigger partition_copy_acl after insert on partition for each row execute procedure partition_copy_acl();

comment on trigger partition_reflect on partition
is E'Any changes made to the partition table should be reflected in the actual partition metadata';


create function trigger_iter(   p_master_class oid,
                                p_range in text default '(,)',
                                p_indent integer default 1)
returns text
language plpgsql set search_path from current as $$
declare
    r record;
    l_lower_range text := 'empty';
    l_upper_range text := 'empty';
    l_range_type text;
begin
    select  range_type::regtype::text
    into strict l_range_type
    from    master
    where   master_class = p_master_class;
    
    for r in execute format('select p.partition_class::regclass::text as partition_name,
                                    p.range,
                                    ( count(*) over () = 1 ) as is_only_partition,
                                    ( row_number() over(order by p.range::%1$s) < ((count(*) over () / 2) + 1) ) as is_lower_half
                            from partition p
                            where p.master_class = $1
                            and p.range::%1$s <@ $2::%1$s
                            order by range::%1$s',
                            l_range_type)
                            using p_master_class, p_range
    loop
        if r.is_only_partition then
            -- there is only one partition, so just insert into it
            return format(E'insert into %s values(new.*);\n', r.partition_name);
        elsif r.is_lower_half then
            -- add this partition to the lower range
            l_lower_range := range_add(l_lower_range, r.range, l_range_type);
        else
            -- add this partition to the upper range, good thing they're already in order
            l_upper_range := range_add(l_upper_range, r.range, l_range_type);
        end if;
    end loop;

    return  format(E'%1$sif new.%2$s <@ %3$L::%4$s then\n%5$s%1$selse\n%6$s%1$send if;',
                    repeat('  ',p_indent),
                    (select partition_attribute from master where master_class = p_master_class),
                    l_lower_range,
                    l_range_type,
                    trigger_iter(p_master_class, l_lower_range, p_indent + 1),
                    trigger_iter(p_master_class, l_upper_range, p_indent + 1));

end
$$;

comment on function trigger_iter(oid, text, integer)
is E'recursive function to do a binary traversal of the partitions in a table,\n'
    'generating IF/THEN tests to find the right partition.';


create function create_table_like(  p_qual_new_table text,
                                    p_qual_model_table text ) returns void
language plpgsql set search_path from current as $$
declare
    l_model_oid oid := p_qual_model_table::regclass;
    l_tablespace text;
begin
    -- see if the model table has a non-default tablespace, if so, use that
    select  t.spcname
    into    l_tablespace
    from    pg_class c
    join    pg_tablespace t
    on      t.oid = c.reltablespace
    where c.oid = l_model_oid;

    if found then
        execute format('create table %s(like %s including all) tablespace %I',
                        p_qual_new_table,
                        p_qual_model_table,
                        l_tablespace);
    else
        execute format('create table %s(like %s including all)',
                        p_qual_new_table,
                        p_qual_model_table);
    end if;
end;
$$;

comment on function create_table_like(  p_qual_new_table text,
                                        p_qual_model_table text )
is E'Create a table like the model table, with same indexes, tablespaces, ownership, permissions';


create function create_parent(  p_qual_table_name text,
                                p_range_column_name text,
                                p_dest_schema text default null,
                                p_qual_range_type text default null) returns void
language plpgsql set search_path from current as $$
declare
    r record;
    l_master_oid oid;
    l_range_type_oid oid;
    l_attribute_oid oid;
begin
    -- validate table
    begin
        l_master_oid := p_qual_table_name::regclass;
    exception
        when invalid_schema_name then
            raise exception '% is an unknown schema', p_qual_table_name;
        when undefined_table then
            raise exception '% is an unknown table', p_qual_table_name;
        when others then raise;
    end;

    -- validate partitioning column
    if not exists(  select  null
                    from    pg_attribute
                    where   attrelid = l_master_oid
                    and     attname = p_range_column_name ) then
        raise exception 'Column % not found on  %', p_range_column_name, p_qual_table_name;
    end if;

    if p_qual_range_type is not null then
        begin
            l_range_type_oid := p_qual_range_type::regtype;
        exception
            when invalid_schema_name then
                raise exception '% is an unknown schema', p_qual_range_type;
            when undefined_object then
                raise exception '% is an unknown type', p_qual_range_type;
            when others then raise;
        end;
        if not exists ( select  null
                        from    pg_attribute a
                        join    pg_range rt
                        on      rt.rngsubtype = a.atttypid
                        and     rt.rngcollation = a.attcollation
                        where   a.attrelid = l_master_oid
                        and     a.attname = p_range_column_name
                        and     rt.rngtypid = l_range_type_oid ) then
            raise exception '% is not a suitable range type for % on %',
                            p_qual_range_type,
                            p_range_column_name,
                            p_qual_table_name;
        end if;
    else
        begin
            select  rt.rngtypid
            into    strict l_range_type_oid
            from    pg_attribute a
            join    pg_range rt
            on      rt.rngsubtype = a.atttypid
            and     rt.rngcollation = a.attcollation
            where   a.attrelid = l_master_oid
            and     a.attname = p_range_column_name;
        exception
            when no_data_found then
                raise exception 'No suitable range type for % on %', p_range_column_name, p_qual_table_name;
            when too_many_rows then
                raise exception 'Multiple range types (%) are valid for column % on %',
                                (   select  string_agg(rt.rngtypid::regtype::text, ', ')
                                    from    pg_attribute a
                                    join    pg_range rt
                                    on      rt.rngsubtype = a.atttypid
                                    and     rt.rngcollation = a.attcollation
                                    where   a.attrelid = l_master_oid
                                    and     a.attname = p_range_column_name ),
                                p_range_column_name,
                                p_qual_table_name
                    using hint = 'Specify one of those types in the p_qual_range_type parameter';
            when others then raise;
        end;
    end if;

    -- find the range type for the partitioning column, must find exactly one, fail otherwise
    select  format('%I.%I',n.nspname,c.relname) as source_table,
            format('%I.%I',coalesce(p_dest_schema,n.nspname),c.relname || '_p0') as partition_table,
            format('%I.%I',coalesce(p_dest_schema,n.nspname),c.relname || '_ins_trigger') as insert_trigger_function,
            format('%I',c.relname || '_ins_trig') as insert_trigger_name
    into    strict r
    from    pg_class c
    join    pg_namespace n
    on      n.oid = c.relnamespace
    where   c.oid = l_master_oid;

    -- create the table that will inherit from the master table
    perform create_table_like(r.partition_table,r.source_table);

    -- create the record and set the name of the trigger function so that it can be created
    insert into master 
    values (l_master_oid, p_range_column_name, l_range_type_oid, r.insert_trigger_function);

    -- inserting a row here will automatically add the constraint on the partition and complete the inheritance
    insert into partition(partition_class,master_class,partition_number,range)
    values (r.partition_table::regclass, l_master_oid, 0, '(,)');

    -- migrate rows to main partition
    execute format('with d as (delete from %s returning *) insert into %s select * from d',
                    r.source_table,
                    r.partition_table);

    execute format('create trigger %s before insert on %s for each row execute procedure %s()',
                    r.insert_trigger_name,
                    p_qual_table_name,
                    r.insert_trigger_function);
end;
$$;

comment on function create_parent(text,text,text,text)
is E'Convert a normal table into the master table of a partition set.';

create function create_partition (  p_qual_table_name text,
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
    perform create_table_like(l_new_partition, mr.master_class::regclass::text);

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
end;
$$;

comment on function create_partition(text,text)
is E'create a new partition by splitting it off from an existing partition.\n'
    'the range given must match the left side or right side of an existing partition or the operation will fail.';


create function drop_partition (p_drop_partition_name text,
                                p_adjacent_partition_name text) returns void 
language plpgsql set search_path from current as $$
declare
    l_range_union text;
    r record;
begin
    -- verify that both partitions exist
    select  m.partition_attribute as range_column,
            m.range_type,
            a.range as drop_range,
            b.range as keep_range,
            m.master_class::regclass::text as qual_master_table_name
    into strict r
    from    partition a
    join    partition b
    on      b.master_class = a.master_class
    join    master m
    on      m.master_class = a.master_class
    where   a.partition_class = p_drop_partition_name::regclass
    and     b.partition_class = p_adjacent_partition_name::regclass;

    -- verify that both partitions are adjacent, get the combined range
    begin
        execute format('select r.x + r.y from( select $1::%1$s as x, $2::%1$s as y ) as r where r.x -|- r.y',
                        r.range_type::regtype::text)
        using r.drop_range, r.keep_range
        into strict l_range_union;
    exception
        when no_data_found then
        raise exception '% cannot be merged into %s because it is not adjacent',
                        p_drop_partition_name,
                        p_adjacent_partition_name;
    end;

    -- delete the doomed entry
    delete from partition where partition_class = p_drop_partition_name::regclass;

    -- reflect the change in the partition table, this will update the constraint as well
    update  partition
    set     range = l_range_union
    where   partition_class = p_adjacent_partition_name::regclass;

    -- move rows from doomed partition to survivor
    execute format('insert into %s select * from %s',
                    p_adjacent_partition_name::regclass::text,
                    p_drop_partition_name::regclass::text);

    -- drop doomed partition
    execute format('drop table %s',
                    p_drop_partition_name::regclass::text);
end;
$$;

comment on function drop_partition (text, text)
is E'merge two adjacent partitions into one single partition';

do $$
begin
    if not exists( select null from pg_roles where rolname = 'range_partitioning') then
        create role range_partitioning;
    end if;
end
$$;

grant select,insert,update, delete on master, partition to range_partitioning;
grant select on master_partition to range_partitioning;

-- grant execute on all functions in this extension to role range_partitioning 
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
end
$$;

