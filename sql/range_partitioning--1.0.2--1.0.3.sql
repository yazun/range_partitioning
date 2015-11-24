
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
                    (   select  format('%I.%I',s.nspname,t.typname)
                        from    pg_type t
                        join    pg_namespace s
                        on      s.oid = t.typnamespace
                        where   t.oid = p_range_type ))
    using   p_range
    into strict lower, upper, empty, lower_inc, upper_inc, lower_inf, upper_inf;
end;
$$;

comment on function range_type_info(text, oid, out text, out boolean, out boolean, out text, out boolean, out boolean)
is E'given a text representation of a range and the name of the range type, create that range\n'
    'and then run the lower(), upper(), lower_inc(), upper_inc(), lower_inf(), and upper_inf() functions';

create or replace function range_type_info(p_range text, p_range_type text, empty out boolean,
                                lower out text, lower_inc out boolean, lower_inf out boolean,
                                upper out text, upper_inc out boolean, upper_inf out boolean)
language sql set search_path from current as $$
select  *
from    range_type_info(p_range,p_range_type::regtype);
$$;

create function where_clause(p_col text, p_range text, p_range_type oid) returns text
language sql set search_path from current as $$
select  case
            when i.lower = i.upper then format('%I = %L',p_col,i.lower)
            when i.lower_inf and i.upper_inf then 'true'
            when i.empty then 'false'
            else    case
                        when i.lower_inf then ''
                        when i.lower_inc then format('%I >= %L',p_col,i.lower)
                        else format('%I > %L',p_col,i.lower)
                    end ||
                    case
                        when not i.lower_inf and not i.upper_inf then ' and ' 
                        else ''
                    end ||
                    case
                        when i.upper_inf then ''
                        when i.upper_inc then format('%I <= %L',p_col,i.upper)
                        else format('%I < %L',p_col,i.upper)
                    end
        end
from    range_type_info(p_range,p_range_type) i;
$$;

comment on function where_clause(text,text,oid)
is E'construct a WHERE clause that would exactly fit the given column, range, and range_type';

create or replace function where_clause(p_col text, p_range text, p_range_type text) returns text
language sql set search_path from current as $$
select  where_clause(p_col,p_range,p_range_type::regtype);
$$;

grant execute on function
    range_type_info(text, oid, out text, out boolean, out boolean, out text, out boolean, out boolean),
    where_clause(text,text,oid)
    to range_partitioning;
 

