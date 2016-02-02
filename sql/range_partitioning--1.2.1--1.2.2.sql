
drop function create_parent(text,text,text);

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
            raise exception '%s is an unknown schema', p_qual_table_name;
        when undefined_table then
            raise exception '%s is an unknown table', p_qual_table_name;
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
                raise exception '%s is an unknown schema', p_qual_range_type;
            when undefined_object then
                raise exception '%s is an unknown type', p_qual_range_type;
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
    execute format('create table %s(like %s including indexes)',
                    r.partition_table,
                    r.source_table);

    -- copy permissions from master
    update  pg_class
    set     relacl = (  select relacl from pg_class where oid = l_master_oid )
    where   oid = r.partition_table::regclass;

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

    perform create_trigger_function(l_master_oid);

    execute format('create trigger %s before insert on %s for each row execute procedure %s()',
                    r.insert_trigger_name,
                    p_qual_table_name,
                    r.insert_trigger_function);
end;
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

    -- copy permissions from master
    update  pg_class
    set     relacl = (  select relacl from pg_class where oid = mr.master_class )
    where   oid = l_new_partition::regclass;

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

