CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_try_etl(exe text) 
	RETURNS json
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
as $body$

declare 
    rc int8;
    txt text;
begin 
    raise info '%', exe;
    begin
        execute exe into txt;
        get diagnostics rc = ROW_COUNT;
    exception when OTHERS then
        raise info '%', exe;
        execute exe;
        get diagnostics rc = ROW_COUNT;
    end;
    raise info '%: %', rc, txt;
    return format('{"rc": %s, "txt": "%s"}', rc, txt)::json;
end; 
$body$
EXECUTE ON ANY;
	