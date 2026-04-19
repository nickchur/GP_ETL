CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_rebuild_movement_detail() 
	RETURNS character varying
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare
    m_txt text;
    log_id int;
    e_detail text;
    e_hint text;
    e_context text;
    tbl text := 's_grnplm_vd_hr_edp_stg.tb_movement_detail';
    ret_var text := 'OK';
begin
    log_id = s_grnplm_vd_hr_edp_srv_wf.pr_Log_start('GPDDS_PR_REBUILD_MOVEMENT_DETAIL');     --Log
    begin
        truncate s_grnplm_vd_hr_edp_stg.tb_movement_detail;

        insert into s_grnplm_vd_hr_edp_stg.tb_movement_detail
        --нахождение необходимых периодов привязок
        with pernr_pos_history as (
            select
                ti.id_connection_object as i_pernr
                , ti.id_object as pos_id
                , ti.date_start
                , ti.date_finish
            from s_grnplm_vd_hr_edp_stg.tb_infotype_1001_sm ti
            where 1 = 1
            and ti.connection_type = 'A008'
        ), pos_jobcode_history as (
            select
                ti.id_connection_object as job_code
                , ti.id_object as pos_id
                , ti.date_start
                , ti.date_finish
            from s_grnplm_vd_hr_edp_stg.tb_infotype_1001_sm ti
            where 1 = 1
            and ti.connection_type = 'B007'
        ), pernr_etpos_history as (
            select
                ti.id_connection_object as et_pos_id
                , ti.id_object as pos_id
                , ti.date_start
                , ti.date_finish
            from s_grnplm_vd_hr_edp_stg.tb_infotype_1001_sm ti
            where 1 = 1
            and ti.connection_type = 'BZ71'
        )
        -- формирование таблицы
        select
            tem.action_date as cal_date
            , tem.i_pernr
            , tem.action_type as code_movement_type
            , tem.action_reason as movement_motiv
            , coalesce(mtf.permanent_movement, 0) as permanent_movement
            , pp.pos_id
            , nullif(pph.pos_id, pp.pos_id) as old_pos_id -- старая должность
            , pj.job_code
            , nullif(pjh.job_code, pj.job_code) as old_job_code -- старый джобкод
            , pe.et_pos_id
            , nullif(peh.et_pos_id, pe.et_pos_id) as old_et_pos_id -- старая эталонная должность
            , tem.org_id
            , nullif(lag(tem.org_id,1) over (win1), tem.org_id) as old_org_id 
        from s_grnplm_vd_hr_edp_stg.tb_employee_movement_cdm tem
        left join s_grnplm_vd_hr_edp_stg.tb_permanent_movement_types mtf
            on 1 = 1
            and tem.action_type = mtf.code_movement_type and tem.action_reason = mtf.movement_motive
        left join pernr_pos_history pp
            on 1 = 1
            and tem.i_pernr = pp.i_pernr
            and tem.action_date between pp.date_start and pp.date_finish
        left join pos_jobcode_history pj
            on 1 = 1
            and pp.pos_id = pj.pos_id
            and tem.action_date between pj.date_start and pj.date_finish
        left join pernr_etpos_history pe
            on 1 = 1
            and pp.pos_id = pe.pos_id
            and tem.action_date between pe.date_start and pe.date_finish
        --left join s_grnplm_vd_hr_edp_stg.dnf_org_tree as ot
        --on 1 = 1 and tem.org_id::int= ot.org_id
        --and tem.action_date = ot.cal_date
        left join pernr_pos_history pph
            on 1 = 1
            and tem.i_pernr = pph.i_pernr
            and tem.action_date - interval '1 day' between pph.date_start and pph.date_finish
        left join pos_jobcode_history pjh
            on 1 = 1
            and pph.pos_id = pjh.pos_id
            and tem.action_date - interval '1 day' between pjh.date_start and pjh.date_finish
        left join pernr_etpos_history peh
            on 1 = 1 and pph.pos_id = peh.pos_id
            and tem.action_date - interval '1 day' 
            between peh.date_start and peh.date_finish
        --left join s_grnplm_vd_hr_edp_stg.dnf_org_tree as oth
        --on 1 = 1
        --and tem.org_id::int = oth.org_id
        --and tem.action_date = oth.cal_date
        --where 1 = 1
        -- and tem.i_pernr = 1586935 --Дима
        -- and tem.i_pernr = 1758543 --я
        -- and tem.i_pernr = 1757233 --Ира
        window win1 as (partition by tem.i_pernr order by tem.action_date);

        -- логирование воркфлоу GPDDS_REBUILD_MOVEMENT_DETAIL

        perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_end(log_id, tbl, 'cal_date',null);         --Log
        return ret_var;

    exception when OTHERS then
        get stacked diagnostics m_txt = MESSAGE_TEXT;
        get stacked diagnostics e_detail = PG_EXCEPTION_DETAIL;
        get stacked diagnostics e_hint = PG_EXCEPTION_HINT;
        get stacked diagnostics e_context = PG_EXCEPTION_CONTEXT;

        perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_error(log_id,m_txt,e_detail,e_hint,e_context) ;
        --Log
        return m_txt;
    end;
end;

$body$
EXECUTE ON ANY;
	