CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_get_format_type(tbl text, fld text) 
	RETURNS text
	LANGUAGE plpgsql
	IMMUTABLE
as $body$

begin
    return (
        select typname::text from pg_catalog.pg_type 
        where typrelid=0 and typnamespace=11 and typarray>0
        and typname::regtype::oid = (
            select format_type(a.atttypid, a.atttypmod)::regtype::oid from pg_catalog.pg_attribute a 
            where a.attrelid = tbl::regclass::oid and a.attname = fld limit 1
        )
        limit 1
    )::text;
end;

$body$
EXECUTE ON ANY;
	