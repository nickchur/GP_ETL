CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.try_cast(cast_type text, cast_text text) 
	RETURNS text
	LANGUAGE plpgsql
	IMMUTABLE
as $body$

declare
    exe text;
    ret text;
begin
    exe = format($sql$ select nullif(%L,'')::%s::text $sql$, cast_text, cast_type);
    execute exe into ret;
    return ret;
exception when OTHERS then
--    raise info 'Error %', exe;
    return null::text;
end;
$body$
EXECUTE ON ANY;
	