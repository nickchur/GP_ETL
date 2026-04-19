CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_update_ai_history_i_pernr() 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare
    log_id int;
    m_txt text; 
    e_detail text;
    e_hint text;
    e_context text;
    rc int8;
    rw_cnt int8;
    tmp_id int;
begin
    log_id = s_grnplm_vd_hr_edp_srv_wf.pr_Log_start('GPUPDATE_ai_history_i_pernr');   --ЛОГИРОВАНИЕ
    begin
        rw_cnt = 0;

        tmp_id = s_grnplm_vd_hr_edp_srv_wf.pr_Log_start('GPUPDATE_ai_history_i_pernr/delete');   --ЛОГИРОВАНИЕ
        delete from s_grnplm_vd_hr_edp_stg.tb_ai_history_i_pernr where i_pernr>=10^7 or i_pernr<1000;
        get diagnostics rc = ROW_COUNT;
        perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_action('end', to_char(rc,'FM999,999,999,999,999,999,999'), tmp_id); --логирование

        tmp_id = s_grnplm_vd_hr_edp_srv_wf.pr_Log_start('GPUPDATE_ai_history_i_pernr/search');   --ЛОГИРОВАНИЕ
        insert into s_grnplm_vd_hr_edp_stg.tb_ai_history_i_pernr
        select a.id,substring(a.contents_uuid_userid from 'search_(\d+)')::int4 as i_pernr,'search'
        from s_grnplm_vd_hr_edp_stg.tb_ai_history a
        left join s_grnplm_vd_hr_edp_stg.tb_ai_history_i_pernr d on d.ai_id = a.id and d.i_pernr_type = 'search' 
        where a.contents_uuid_userid similar to 'search_[0-9]{4,7}'
        --left(a.contents_uuid_userid,7) = 'search_' and a.contents_uuid_userid <> 'search_null'
            and d.ai_id is null
        ;
        get diagnostics rc = ROW_COUNT;
        rw_cnt = rw_cnt + rc;
        perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_action('end', to_char(rc,'FM999,999,999,999,999,999,999'), tmp_id); --логирование


        tmp_id = s_grnplm_vd_hr_edp_srv_wf.pr_Log_start('GPUPDATE_ai_history_i_pernr/employeeid');   --ЛОГИРОВАНИЕ
        insert into s_grnplm_vd_hr_edp_stg.tb_ai_history_i_pernr
        select a.ai_id,s_grnplm_vd_hr_edp_srv_wf.try_cast2int(nullif(payload_value,''))::int4,'employeeid'
        from s_grnplm_vd_hr_edp_stg.tb_ai_history_payload a
        left join s_grnplm_vd_hr_edp_stg.tb_ai_history_i_pernr d on d.ai_id = a.ai_id and d.i_pernr_type = 'employeeid' 
        where payload_key = 'employeeId' 
        --    and s_grnplm_vd_hr_edp_srv_wf.try_cast2int(nullif(payload_value,''))::int8 is not null
            and s_grnplm_vd_hr_edp_srv_wf.try_cast2int(nullif(payload_value,''))::int8 between 1000 and 9999999
            and d.ai_id is null
        ;
        get diagnostics rc = ROW_COUNT;
        rw_cnt = rw_cnt + rc;
        perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_action('end', to_char(rc,'FM999,999,999,999,999,999,999'), tmp_id); --логирование

        -- tmp_id = s_grnplm_vd_hr_edp_srv_wf.pr_Log_start('GPUPDATE_ai_history_i_pernr/contents_uuid_userid');   --ЛОГИРОВАНИЕ
        -- insert into s_grnplm_vd_hr_edp_stg.tb_ai_history_i_pernr
        -- select a.id , b.data_employeeid as i_pernr,'contents_uuid_userid'
        -- from s_grnplm_vd_hr_edp_stg.tb_ai_history a
        -- inner join (
        --     select d.personid ,Max(d.data_employeeid) data_employeeid
        --     from s_grnplm_vd_hr_edp_stg.tb_pulse_j_basic d
        --     where data_employeeid > 0 
        --     group by 1
        --     having count(distinct d.data_employeeid) = 1
        -- ) b on a.contents_uuid_userid::uuid = b.personid::uuid
        -- left join s_grnplm_vd_hr_edp_stg.tb_ai_history_i_pernr d on d.ai_id = a.id and d.i_pernr_type = 'contents_uuid_userid' 
        -- where length(contents_uuid_userid)=36 and contents_uuid_userid similar to '[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}'  
        --     and b.data_employeeid between 1000 and 9999999
        --     and d.ai_id is null
        -- ;
        -- get diagnostics rc = ROW_COUNT;
        -- rw_cnt = rw_cnt + rc;
        -- perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_action('end', to_char(rc,'FM999,999,999,999,999,999,999'), tmp_id); --логирование

        -- tmp_id = s_grnplm_vd_hr_edp_srv_wf.pr_Log_start('GPUPDATE_ai_history_i_pernr/epkid');   --ЛОГИРОВАНИЕ
        -- insert into s_grnplm_vd_hr_edp_stg.tb_ai_history_i_pernr
        -- select a.ai_id, b.i_pernr, 'epkid'
        -- --,payload_value
        -- from s_grnplm_vd_hr_edp_stg.tb_ai_history_payload a
        -- inner join (
        --     select epk_id, max(i_pernr) i_pernr 
        --     from s_grnplm_vd_hr_edp_stg.tb_vw_epk_id d
        --     --where epk_id=1130254754802123001
        --     group by 1
        --     having count(distinct i_pernr)=1
        -- ) b on a.payload_value = b.epk_id::text
        -- left join s_grnplm_vd_hr_edp_stg.tb_ai_history_i_pernr d on d.ai_id = a.ai_id and d.i_pernr_type = 'epkid' 
        -- where a.payload_key = 'epkId'
        --     and b.i_pernr between 1000 and 9999999
        --     and d.ai_id is null
        -- ;
        -- get diagnostics rc = ROW_COUNT;
        -- rw_cnt = rw_cnt + rc;
        -- perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_action('end', to_char(rc,'FM999,999,999,999,999,999,999'), tmp_id); --логирование


--        tmp_id = s_grnplm_vd_hr_edp_srv_wf.pr_Log_start('GPUPDATE_ai_history_i_pernr/employee_profile');   --ЛОГИРОВАНИЕ
--        insert into s_grnplm_vd_hr_edp_stg.tb_ai_history_i_pernr
--        select b.ai_id, (payload_value::jsonb->'profileInfo'->>'personalNumber')::int4,'employee_profile'
--        --,payload_value::jsonb->'profileInfo'->>'personalNumber'
--        from s_grnplm_vd_hr_edp_stg.tb_ai_history_payload b
--        left join s_grnplm_vd_hr_edp_stg.tb_ai_history_i_pernr d on d.ai_id = b.ai_id and d.i_pernr_type = 'employee_profile' 
--        where payload_key = 'employee_profile'
--            and  s_grnplm_vd_hr_edp_srv_wf.try_cast2jsonb(payload_value) is not null
--            -- and (payload_value::jsonb->'profileInfo'->>'personalNumber')::int8 is not null
--            and (payload_value::jsonb->'profileInfo'->>'personalNumber')::int8 between 1000 and 9999999
--            --and (payload_value::jsonb->'absenceInfo'->'status'->>'code')::int4 = 0
--            and d.ai_id is null
--        ;
--        get diagnostics rc = ROW_COUNT;
--        rw_cnt = rw_cnt + rc;
--        perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_action('end', to_char(rc,'FM999,999,999,999,999,999,999'), tmp_id); --логирование


        -- insert into s_grnplm_vd_hr_edp_stg.tb_ai_history_i_pernr
        -- select a.id,c.i_pernr,'mail_omega'
        -- --,d.i_pernr , a.data_date, c.mail, b.payload_value::json->>'electronicAddresses' as ar 
        -- from s_grnplm_vd_hr_edp_stg.tb_ai_history a
        -- inner join s_grnplm_vd_hr_edp_stg.tb_ai_history_payload b on a.id = b.ai_id 
        -- inner join s_grnplm_vd_hr_edp_stg.vw_mail_mapping c 
        --     on substring(lower(b.payload_value::json->>'electronicAddresses') from '"electronicaddress": "([\w\.\-]+@[\w\.\-]+\.sbrf\.ru)"') = c.mail 
        --     and  a.data_date between coalesce(c.bgn_date,'2000-01-01') and coalesce(c.end_date,current_date)
        -- left join s_grnplm_vd_hr_edp_stg.tb_ai_history_i_pernr d on d.ai_id = a.id and d.i_pernr_type = 'mail_omega' 
        -- where  b.payload_key = 'client_profile'
        --     and (b.payload_value::json->>'electronicAddresses') is not null
        --     and d.ai_id is null;
        -- get diagnostics rc = ROW_COUNT;
        -- rw_cnt = rw_cnt + rc;


        -- insert into s_grnplm_vd_hr_edp_stg.tb_ai_history_i_pernr
        -- select a.id,c.i_pernr,'mail_sberbank'
        -- --, a.data_date, c.mail, b.payload_value::json->>'electronicAddresses' as ar 
        -- from s_grnplm_vd_hr_edp_stg.tb_ai_history a
        -- inner join s_grnplm_vd_hr_edp_stg.tb_ai_history_payload b on a.id = b.ai_id 
        -- inner join s_grnplm_vd_hr_edp_stg.vw_mail_mapping c 
        --     on substring(lower(b.payload_value::json->>'electronicAddresses') from '"electronicaddress": "([\w\.\-]+@sberbank\.ru)"') = c.mail 
        --     and  a.data_date between coalesce(c.bgn_date,'2000-01-01') and coalesce(c.end_date,current_date)
        -- left join s_grnplm_vd_hr_edp_stg.tb_ai_history_i_pernr d on d.ai_id = a.id and d.i_pernr_type = 'mail_sberbank' 
        -- where  b.payload_key = 'client_profile'
        --     and (b.payload_value::json->>'electronicAddresses') is not null
        --     and d.ai_id is null;
        -- get diagnostics rc = ROW_COUNT;
        -- rw_cnt = rw_cnt + rc;

        -- tmp_id = s_grnplm_vd_hr_edp_srv_wf.pr_Log_start('GPUPDATE_ai_history_i_pernr/ucp_id');   --ЛОГИРОВАНИЕ
        -- insert into s_grnplm_vd_hr_edp_stg.tb_ai_history_i_pernr
        -- select a.ai_id , b.i_pernr , 'ucp_id'
        -- from s_grnplm_vd_hr_edp_stg.tb_ai_history_payload a
        --  join (
        --     select epk_id, max(i_pernr) i_pernr 
        --     from s_grnplm_vd_hr_edp_stg.tb_vw_epk_id d
        --     group by 1
        --     having count(distinct i_pernr)=1
        -- ) b on payload_value::jsonb->>'ucp_id' = b.epk_id::text
        -- left join s_grnplm_vd_hr_edp_stg.tb_ai_history_i_pernr d on d.ai_id = a.ai_id and d.i_pernr_type = 'ucp_id' 
        -- where payload_key = 'esa'
        --     and d.ai_id is null
        -- ;
        -- get diagnostics rc = ROW_COUNT;
        -- rw_cnt = rw_cnt + rc;
        -- perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_action('end', to_char(rc,'FM999,999,999,999,999,999,999'), tmp_id); --логирование

        tmp_id = s_grnplm_vd_hr_edp_srv_wf.pr_Log_start('GPUPDATE_ai_history_i_pernr/sessionid');   --ЛОГИРОВАНИЕ
        insert into s_grnplm_vd_hr_edp_stg.tb_ai_history_i_pernr
        select a.id,b.i_pernr,'sessionid' 
        from s_grnplm_vd_hr_edp_stg.tb_ai_history a
        inner join (
            select a.sessionid,max(b.i_pernr) i_pernr
            from s_grnplm_vd_hr_edp_stg.tb_ai_history a
            inner join s_grnplm_vd_hr_edp_stg.tb_ai_history_i_pernr b on a.id = b.ai_id 
            where contents_uuid_userid similar to '[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}'  
            group by 1
            having count(distinct b.i_pernr)=1 
                -- and count(distinct contents_uuid_userid)=1  
        ) b on a.sessionid = b.sessionid 
        left join s_grnplm_vd_hr_edp_stg.tb_ai_history_i_pernr c on a.id = c.ai_id
        where c.ai_id is null
        ;
        get diagnostics rc = ROW_COUNT;
        rw_cnt = rw_cnt + rc;
        perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_action('end', to_char(rc,'FM999,999,999,999,999,999,999'), tmp_id); --логирование

        tmp_id = s_grnplm_vd_hr_edp_srv_wf.pr_Log_start('GPUPDATE_ai_history_i_pernr/uuid_userid');   --ЛОГИРОВАНИЕ
        insert into s_grnplm_vd_hr_edp_stg.tb_ai_history_i_pernr
        select a.id,b.i_pernr,'uuid_userid'  
        from s_grnplm_vd_hr_edp_stg.tb_ai_history a
        inner join (
            select a.contents_uuid_userid,max(b.i_pernr) i_pernr
            from s_grnplm_vd_hr_edp_stg.tb_ai_history a
            inner join s_grnplm_vd_hr_edp_stg.tb_ai_history_i_pernr b on a.id = b.ai_id 
            where contents_uuid_userid similar to '[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}'  
            group by 1
            having count(distinct b.i_pernr)=1
        ) b on a.contents_uuid_userid = b.contents_uuid_userid 
        left join s_grnplm_vd_hr_edp_stg.tb_ai_history_i_pernr c on a.id = c.ai_id
        where c.ai_id is null
        ;
        get diagnostics rc = ROW_COUNT;
        rw_cnt = rw_cnt + rc;
        perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_action('end', to_char(rc,'FM999,999,999,999,999,999,999'), tmp_id); --логирование

        tmp_id = s_grnplm_vd_hr_edp_srv_wf.pr_Log_start('GPUPDATE_ai_history_i_pernr/i_pernr');   --ЛОГИРОВАНИЕ
        insert into s_grnplm_vd_hr_edp_stg.tb_ai_history_i_pernr
        select a.id,b.i_pernr,'i_pernr' 
        --, a.contents_uuid_userid
        from s_grnplm_vd_hr_edp_stg.tb_ai_history a
        inner join (
            select distinct a.contents_uuid_userid, b.i_pernr
            from s_grnplm_vd_hr_edp_stg.tb_ai_history a
            inner join s_grnplm_vd_hr_edp_stg.tb_ai_history_i_pernr b on a.id = b.ai_id 
                and substring(contents_uuid_userid from '[a-z_A-Z]*([0-9]{4,7})')::int4=b.i_pernr
            where contents_uuid_userid similar to '[a-z_A-Z]*[0-9]{4,7}'  
            ) b on a.contents_uuid_userid = b.contents_uuid_userid 
        left join s_grnplm_vd_hr_edp_stg.tb_ai_history_i_pernr c on a.id = c.ai_id
        where c.ai_id is null
        ;
        get diagnostics rc = ROW_COUNT;
        rw_cnt = rw_cnt + rc;
        perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_action('end', to_char(rc,'FM999,999,999,999,999,999,999'), tmp_id); --логирование

        tmp_id = s_grnplm_vd_hr_edp_srv_wf.pr_Log_start('GPUPDATE_ai_history_i_pernr/userid');   --ЛОГИРОВАНИЕ
        insert into s_grnplm_vd_hr_edp_stg.tb_ai_history_i_pernr
        select a.id,b.i_pernr,'userid' 
        --, b.*
        from s_grnplm_vd_hr_edp_stg.tb_ai_history a
        inner join (
            select a.contents_uuid_userid,max(b.i_pernr) i_pernr
                --,count(distinct b.ai_id) id_cnt,string_agg(distinct b.i_pernr_type, ' '),count(distinct a.sessionid) sessionid_cnt
            from s_grnplm_vd_hr_edp_stg.tb_ai_history a
            inner join s_grnplm_vd_hr_edp_stg.tb_ai_history_i_pernr b on a.id = b.ai_id 
            where b.i_pernr_type <> 'true'
                and contents_uuid_userid not similar to '[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}'  
                -- and contents_uuid_userid not similar to '[a-z_A-Z]*[0-9]{4,7}'  
            group by 1
            having count(distinct b.i_pernr)=1
                and count(distinct a.sessionid)>=3
                -- and count(distinct b.i_pernr_type)>1
        ) b on a.contents_uuid_userid = b.contents_uuid_userid 
        left join s_grnplm_vd_hr_edp_stg.tb_ai_history_i_pernr c on a.id = c.ai_id
        where c.ai_id is null
        ;
        get diagnostics rc = ROW_COUNT;
        rw_cnt = rw_cnt + rc;
        perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_action('end', to_char(rc,'FM999,999,999,999,999,999,999'), tmp_id); --логирование
        
        m_txt = 'OK update '||to_char(rw_cnt,'FM999,999,999,999,999,999,999');


        tmp_id = s_grnplm_vd_hr_edp_srv_wf.pr_Log_start('GPUPDATE_ai_history_i_pernr/true');   --ЛОГИРОВАНИЕ
        truncate s_grnplm_vd_hr_edp_stg.tb_ai_history_i_pernr_1_prt_true;

        insert into s_grnplm_vd_hr_edp_stg.tb_ai_history_i_pernr
        select b.ai_id, max(i_pernr), 'true'
        from s_grnplm_vd_hr_edp_stg.tb_ai_history_i_pernr b 
        group by 1
        having count(distinct i_pernr)=1
        ;
        get diagnostics rc = ROW_COUNT;
        perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_action('end', to_char(rc,'FM999,999,999,999,999,999,999'), tmp_id); --логирование

        m_txt = m_txt||'; True '||to_char(rc,'FM999,999,999,999,999,999,999');

        tmp_id = s_grnplm_vd_hr_edp_srv_wf.pr_Log_start('GPUPDATE_ai_history_i_pernr/false');   --ЛОГИРОВАНИЕ
        truncate s_grnplm_vd_hr_edp_stg.tb_ai_history_i_pernr_1_prt_false;

        insert into s_grnplm_vd_hr_edp_stg.tb_ai_history_i_pernr
        select b.ai_id, max(i_pernr), 'false'
        from s_grnplm_vd_hr_edp_stg.tb_ai_history_i_pernr b 
        group by 1
        having count(distinct i_pernr)>1
        ;
        get diagnostics rc = ROW_COUNT;
        perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_action('end', to_char(rc,'FM999,999,999,999,999,999,999'), tmp_id); --логирование

        m_txt = m_txt||'; False '||to_char(rc,'FM999,999,999,999,999,999,999');

        perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_skew('s_grnplm_vd_hr_edp_stg.tb_ai_history_i_pernr'); 
        perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_action('end', m_txt, log_id); --логирование
        return m_txt;

    exception when OTHERS then
        get stacked diagnostics m_txt = MESSAGE_TEXT;
        get stacked diagnostics e_detail = PG_EXCEPTION_DETAIL;
        get stacked diagnostics e_hint = PG_EXCEPTION_HINT;
        get stacked diagnostics e_context = PG_EXCEPTION_CONTEXT;

        perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_error(log_id,m_txt,e_detail,e_hint,e_context);

        RETURN m_txt;
    end; 
end;

$body$
EXECUTE ON ANY;
	

COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_update_ai_history_i_pernr() IS 'Связывает записи tb_ai_history с табельными номерами сотрудников по нескольким алгоритмам идентификации';
