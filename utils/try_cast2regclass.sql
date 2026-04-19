CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.try_cast2regclass(cast_text text, debug boolean DEFAULT false) 
	RETURNS regclass
	LANGUAGE plpgsql
	IMMUTABLE
as $body$

	
begin
    return nullif(cast_text,'')::regclass; 
exception when OTHERS then
    if debug is True then
        raise info 'Error %', cast_text;
    end if;
    return null::regclass;
end;


$body$
EXECUTE ON ANY;

COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_wf.try_cast2regclass(text, boolean) IS 'Безопасное приведение текста к regclass, возвращает NULL при ошибке';
	