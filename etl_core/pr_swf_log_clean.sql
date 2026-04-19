CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_swf_log_clean(p_action text DEFAULT NULL::text, p_date date DEFAULT (('now'::text)::date - 3)) 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare 
	sql text;
	rc int8;
	ret text;
	txt text;
begin
--	delete from s_grnplm_vd_hr_edp_srv_wf.tb_swf_stat_activity a where a.now < current_date-3; --p_date;
--	get diagnostics rc = ROW_COUNT;
--	txt = s_grnplm_vd_hr_edp_srv_wf.pr_log_skew('s_grnplm_vd_hr_edp_srv_wf.tb_swf_stat_activity');
-- 	raise info '%', txt;
	
	sql = 'end_action in (select trim(regexp_split_to_table(''' || p_action || ''', '',''))) and ';
	
	sql = format($sql$
	delete from s_grnplm_vd_hr_edp_srv_wf.tb_swf__log
	where id in (
		select beg_id id from s_grnplm_vd_hr_edp_srv_wf.vw_swf__log
		where %1$s beg_action < '%2$s'
		union all
		select end_id id from s_grnplm_vd_hr_edp_srv_wf.vw_swf__log
		where %1$s beg_action < '%2$s'
	)
	$sql$, coalesce(sql, ''), p_date::text);
	raise info '%', sql;

	execute sql;
	get diagnostics rc = ROW_COUNT;
	txt = s_grnplm_vd_hr_edp_srv_wf.pr_log_skew('s_grnplm_vd_hr_edp_srv_wf.tb_swf__log');
 	raise info '%', txt;

	ret = format('Ok del - %s', rc);
	raise info '%', ret;
	return ret;

exception when OTHERS then
	declare 
		e_txt text;
	    e_detail text;
	    e_hint text;
	    e_context text;
	begin    
	    get stacked diagnostics e_txt = MESSAGE_TEXT;
	    get stacked diagnostics e_detail = PG_EXCEPTION_DETAIL;
	    get stacked diagnostics e_hint = PG_EXCEPTION_HINT;
	    get stacked diagnostics e_context = PG_EXCEPTION_CONTEXT;
	    
	    perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_error(0, e_txt, e_detail, e_hint, e_context); 
	    return e_txt;
   end;
end;

$body$
EXECUTE ON ANY;
	