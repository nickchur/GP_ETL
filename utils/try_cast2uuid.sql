CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.try_cast2uuid(cast_text text, debug boolean DEFAULT false) 
	RETURNS uuid
	LANGUAGE plpgsql
	IMMUTABLE
as $body$

	
begin
    return nullif(cast_text,'')::uuid; 
exception when OTHERS then
    if debug is True then
        raise info 'Error %', cast_text;
    end if;
    return null::uuid;
end;


$body$
EXECUTE ON ANY;

COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_wf.try_cast2uuid(text, boolean) IS 'Безопасное приведение текста к uuid, возвращает NULL при ошибке';
