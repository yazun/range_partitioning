
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

