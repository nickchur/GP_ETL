CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.try_hash(hash_text text) 
	RETURNS uuid
	LANGUAGE plpgsql
	IMMUTABLE
as $body$

begin
    return md5(public.digest('6aa2ce6e962fa075776eff9ed13778ee'||hash_text,'sha256'))::uuid; 
exception when OTHERS then
    return null::uuid;
end;
$body$
EXECUTE ON ANY;

COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_wf.try_hash(text) IS 'Вычисляет детерминированный UUID-хэш строки на основе SHA-256 с солью';
