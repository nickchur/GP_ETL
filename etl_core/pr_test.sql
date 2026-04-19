CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_test() 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

begin
    set search_path to s_grnplm_vd_hr_edp_srv_wf;
end;

$body$
EXECUTE ON ANY;
	
CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_test(tp integer) 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare 
    log_id int4;
    txt text;
begin
    set search_path to s_grnplm_vd_hr_edp_srv_wf;
    GET diagnostics txt = PG_CONTEXT;
    txt = substring(split_part(txt,'\n', 1) from 'PL/pgSQL function ([\w\.]+)\([ \w\,]*\) line \d+ at GET DIAGNOSTICS');
    txt = coalesce(nullif(split_part(txt,'.',2),''), split_part(txt,'.',1));
    log_id = pr_Log_start(txt);
    begin
        if tp = -1 then
            truncate s_grnplm_vd_hr_edp_dia.tmp_test;
        elsif tp = 0 then
            delete from s_grnplm_vd_hr_edp_dia.tmp_test where 1=1;
        elsif tp = 1 then
            perform pr_log_skew('s_grnplm_vd_hr_edp_dia.tmp_test');
        else 
            perform count(distinct ts) from s_grnplm_vd_hr_edp_dia.tmp_test where 1=1;
            perform pg_sleep(60);
        end if;

        log_id = pr_Log_end(log_id,'s_grnplm_vd_hr_edp_dia.tmp_test','ts',null,null); --ЛОГИРОВАНИЕ
        return 'Ok '||(clock_timestamp()-now())::interval(0)::text;
    
    exception when OTHERS then
        declare e_detail text; e_hint text; e_context text;
        begin
            get stacked diagnostics e_detail = PG_EXCEPTION_DETAIL, e_hint = PG_EXCEPTION_HINT, e_context = PG_EXCEPTION_CONTEXT;

            log_id = pr_Log_error(0, sqlerrm, e_detail, e_hint, e_context);
            return concat('Error: ', sqlerrm);
            -- raise exception using ERRCODE = sqlstate, MESSAGE = sqlerrm, DETAIL = e_detail, HINT = e_hint;            
         end;
    end;    
end; 
$body$
EXECUTE ON ANY;
	