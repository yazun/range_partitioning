
create unique index on master(insert_trigger_function);

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

create or replace function partition_reflect() returns trigger
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

    -- after trigger, no need to return anything
    return null;
end
$$;


create or replace function create_trigger_function(p_master_class oid) returns void
language plpgsql as $$
begin
    execute format(E'create or replace function %s() returns trigger language plpgsql as $BODY$\n'
                    'begin\n%sreturn null;\nend;$BODY$',
                    ( select insert_trigger_function from master where master_class = p_master_class ),
                    trigger_iter(p_master_class));
end;
$$;

comment on function create_trigger_function(oid)
is E'(re)create a trigger function for the given table. This is run as a part of adding/removing partitions.';

create or replace function create_table_like(  p_qual_new_table text,
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

create or replace function create_parent(  p_qual_table_name text,
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

    perform create_trigger_function(l_master_oid);

    execute format('create trigger %s before insert on %s for each row execute procedure %s()',
                    r.insert_trigger_name,
                    p_qual_table_name,
                    r.insert_trigger_function);

end;
$$;

create function drop_parent( p_qual_table_name text ) returns void
language plpgsql set search_path from current as $$
declare
    r record;
begin
    select  m.*,
            format('%I',c.relname || '_ins_trig') as insert_trigger_name
    into    r
    from    master m
    join    pg_class c
    on      c.oid = m.master_class
    where   m.master_class = p_qual_table_name::regclass;

    delete from partition where master_class = r.master_class;

    execute format('drop function if exists %s()', r.insert_trigger_function);

    execute format('drop trigger if exists %s on %s', r.insert_trigger_name, p_qual_table_name );

    delete from master where master_class = r.master_class;
end;
$$;

comment on function drop_parent( p_qual_table_name text )
is E'Stop management of the table as a range-partitioned table. Leave existing tables as-is';

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

