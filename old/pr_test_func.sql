CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_test_func() 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare 
    txt text;
begin 
    raise info '%', clock_timestamp();
    GET diagnostics txt = PG_CONTEXT;
    raise info '%', txt;
    txt = substring(split_part(txt,'\n', 1) from 'PL/pgSQL function ([\w\.]+)\([ \w\,]*\) line \d+ at GET DIAGNOSTICS');
    txt = coalesce(nullif(split_part(txt,'.',2),''), split_part(txt,'.',1));
    raise info '%', txt;
    return txt;
end; 
$body$
EXECUTE ON ANY;
	