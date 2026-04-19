CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.try_cast2bool(cast_text text, debug boolean DEFAULT false) 
	RETURNS boolean
	LANGUAGE plpgsql
	IMMUTABLE
as $body$

	
begin
    return nullif(cast_text,'')::bool; 
exception when OTHERS then
    if debug is True then
        raise info 'Error %', cast_text;
    end if;
    return null::bool;
end;


$body$
EXECUTE ON ANY;

COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_wf.try_cast2bool(text, boolean) IS 'Безопасное приведение текста к boolean, возвращает NULL при ошибке';
