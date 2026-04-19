CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.set_search_path(s_p text) 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare 
    txt text;
begin 
    execute 'show search_path' into txt;
    raise info '% -> %', txt, s_p;
    
    execute format('set search_path to %s', s_p);
    return txt;
end; 

$body$
EXECUTE ON ANY;

COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_wf.set_search_path(text) IS 'Устанавливает search_path и возвращает предыдущее значение';
