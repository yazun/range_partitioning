create function get_collation(p_master_class oid) returns text language sql as $$
select  l.collname::text
from    master m
join    pg_range r
on      r.rngtypid = m.range_type
left
join    pg_collation l
on      l.oid = r.rngcollation
where   m.master_class = p_master_class;
$$;

comment on function get_collation(p_master_class oid)
is 'return the text name of the collation for this partition key, if any';

create function get_exclusion_constraint_name(p_partition_class oid) returns text language sql strict as $$
select  c.relname::text
from    pg_class c
where   c.oid = p_partition_class;
$$;

comment on function get_exclusion_constraint_name(p_partition_class oid)
is 'generate the name of the exclusion constraint for this partition';

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
                    (   select  where_clause(m.partition_attribute,
                                            p.range,
                                            m.range_type::regtype::text,
                                            get_collation(m.master_class))
                        from    partition p
                        join    master m
                        on      m.master_class = p.master_class
                        where   p.partition_class = p_partition_class));
end
$$;

comment on function create_exclusion_constraint(p_partition_class oid)
is 'create the exclusion constraint for a given partition';

create function refresh_exclusion_constraint(p_partition_class oid) returns boolean language plpgsql as $$
begin
    perform drop_exclusion_constraint(p_partition_class);
    perform create_exclusion_constraint(p_partition_class);
    return true;
end
$$;

comment on function refresh_exclusion_constraint(p_partition_class oid)
is 'drop and recreate exclusion constraints';

create or replace function partition_reflect() returns trigger
language plpgsql as $$
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

    return new;
end
$$;

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

create or replace function where_clause(p_partition_class oid) returns text
language sql set search_path from current as $$
select  where_clause(m.partition_attribute,p.range,m.range_type,get_collation(p.master_class))
from    partition p
join    master m
on      m.master_class = p.master_class
where   p.partition_class = p_partition_class;
$$;

drop function where_clause(text,text,oid);
drop function where_clause(text,text,text);

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
end;
$$;

