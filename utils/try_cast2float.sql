CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.try_cast2float(cast_text text, debug boolean DEFAULT false) 
	RETURNS double precision
	LANGUAGE plpgsql
	IMMUTABLE
as $body$

begin
    return nullif(cast_text,'')::float; 
exception when OTHERS then
    if debug is True then
        raise info 'Error %', cast_text;
    end if;
    return null::float;
end;

$body$
EXECUTE ON ANY;

COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_wf.try_cast2float(text, boolean) IS 'Безопасное приведение текста к double precision, возвращает NULL при ошибке';
