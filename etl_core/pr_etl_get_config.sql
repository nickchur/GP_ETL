CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_etl_get_config(wf text) 
	RETURNS record
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare
    rec record;
    rw int8;
begin
    SET search_path = s_grnplm_vd_hr_edp_srv_wf;
       
    select a.* into rec from tb_etl_config a where a.workflow=wf;

    get diagnostics rw = ROW_COUNT;
   
    if (rw<1) then return rec; end if;
    if (rw>1) then return null; end if;

    return rec;

exception when OTHERS then
    perform pr_Log_error(null,SQLERRM) ; --ЛОГИРОВАНИЕ
    return null;
END;

$body$
EXECUTE ON ANY;
	