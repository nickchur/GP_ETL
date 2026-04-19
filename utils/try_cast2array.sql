CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.try_cast2array(cast_text text, debug boolean DEFAULT false) 
	RETURNS text[]
	LANGUAGE plpgsql
	IMMUTABLE
as $body$

begin
    return nullif(cast_text,'')::text[]; 
exception when OTHERS then
	begin
	    return nullif(translate(cast_text,'[]{}','{}[]'),'')::text[]; 
	exception when OTHERS then
	    if debug is True then
	        raise info 'Error %', cast_text;
	    end if;
	    return null::text[];
	end;
end;

$body$
EXECUTE ON ANY;
	