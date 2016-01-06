SET client_min_messages = warning;
begin;
do $$
declare r record;
begin
    for r in (  select p.proname, pg_get_function_identity_arguments(p.oid) as args
                from   pg_proc p
                join   pg_depend d on d.objid = p.oid and d.deptype = 'e'
                join   pg_extension x on x.oid = d.refobjid
                where   x.extname = 'range_partitioning' )
    loop
        execute format('drop function %s(%s) cascade',r.proname,r.args);
    end loop;
end
$$;
drop table partition cascade;
drop table master cascade;
commit;
