CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_etl_source_to_target(wf text) 
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
    etl record;
    rw int8;
    sql text;
    key_date text;
BEGIN
    SET search_path = s_grnplm_vd_hr_edp_srv_wf;
    log_id = pr_log_start(wf);   --ЛОГИРОВАНИЕ
    begin
        etl =  pr_etl_get_config(wf);
        if etl.workflow is null then
        	m_txt = wf||' etl_config not found';
            perform pr_Log_error(log_id,m_txt, '', '', '') ; --ЛОГИРОВАНИЕ
            return m_txt;
        end if;
   
        execute 'select * from '||etl.source_table||' limit 1';
        get diagnostics rw = ROW_COUNT;
        raise INFO '%',rw;
        if rw <= 0  then
            m_txt = 'EMPTY '||etl.source_table;
            perform pr_Log_error(log_id,m_txt, '', '', '') ; --ЛОГИРОВАНИЕ
            return m_txt;
        end if;
       
        m_txt = 'Ok '|| etl.proc_type;

        select case 
        when etl.proc_type = 'TRUNCATE' then
            'truncate '||etl.target_table||'; '
        when etl.proc_type = 'UPDATE'then
            'delete from '||etl.target_table
            ||' where '||etl.key_field||'::'||etl.key_type
            ||' in (select distinct '||etl.key_field||'::'||etl.key_type
            ||' from '||etl.source_table||'); '
        else null end into sql;

        if sql is not null then
            raise INFO '%',sql;
            execute sql;
            if etl.proc_type = 'UPDATE' then
	            get diagnostics rw = ROW_COUNT;
	            raise INFO '%',rw;
            	m_txt = m_txt|| ' del '||rw::text;
            end if;
        end if;

        select case 
        when etl.proc_type in ('TRUNCATE','UPDATE','INSERT') then
            'insert into '||etl.target_table||COALESCE(' ('||nullif(etl.target_fields,'')||')','')
            ||' select '||case when etl.source_distinct then 'distinct ' else ''end||COALESCE(''||nullif(etl.source_fields,''),'*')
            ||' from '||COALESCE(' ('||nullif(etl.source_sql,'')||') a',etl.source_table)||';'
        when etl.proc_type = 'EXCEPT' then
            'insert into '||etl.target_table||COALESCE(' ('||nullif(etl.target_fields,'')||')','')
            ||' select '||case when etl.source_distinct then 'distinct ' else ''end||COALESCE(''||nullif(etl.source_fields,''),'*')
            ||' from '||COALESCE(' ('||nullif(etl.source_sql,'')||') a',etl.source_table)
            ||' except'
            ||' select '||COALESCE(''||nullif(etl.exc_fields,''),'*')
            ||' from '||etl.target_table||';'
--           when etl.proc_type = 'MANUAL' then
--                  etl.source_sql::text
        else null end into sql;

        if sql is not null then
            raise INFO '%',sql;
            execute sql;
            get diagnostics rw = ROW_COUNT;
            raise INFO '%',rw;
            m_txt = m_txt|| ' ins '||rw::text;
        end if;

        perform pr_log_skew(etl.target_table::text);


        if lower(etl.key_type) = 'date' then
            key_date=etl.key_field;
        else 
--            key_date=null;
            key_date=etl.key_field;
        end if;

        -- ККД
        -- отчетная дата - макс. дата
        if nullif(etl.dt_field,'') is not null then
	        perform s_grnplm_vd_hr_edp_srv_dq.fn_dq_calc_max_date(
	            current_date
	            , etl.target_table::text
	            , etl.dt_field::text
	            , etl.target_table||' '||etl.dt_field||' max_date'
	            , etl.dt_period::text
	        );
       	end if;
        -- /ККД
             
        perform pr_log_end(log_id,etl.source_table::text,nullif(etl.dt_field,'')::text
        					,nullif(etl.load_field,'')::text,nullif(key_date,'')::text); --ЛОГИРОВАНИЕ

        if (etl.truncate_after is true) then
            execute 'TRUNCATE '||etl.source_table;
        end if;

        return m_txt;

    exception when OTHERS then
        get stacked diagnostics m_txt = MESSAGE_TEXT;
        get stacked diagnostics e_detail = PG_EXCEPTION_DETAIL;
        get stacked diagnostics e_hint = PG_EXCEPTION_HINT;
        get stacked diagnostics e_context = PG_EXCEPTION_CONTEXT;

        perform pr_log_error(log_id,m_txt,e_detail,e_hint,e_context) ; --ЛОГИРОВАНИЕ
--        if is_notice then
--            raise NOTICE 'MESSAGE_TEXT %',m_txt;
--            raise NOTICE 'PG_EXCEPTION_DETAIL %',e_detail;
--            raise NOTICE 'PG_EXCEPTION_HINT %',e_hint;
--            raise NOTICE 'PG_EXCEPTION_CONTEXT %',e_context;
--        end if;
        return m_txt;
    end;
END;

$body$
EXECUTE ON ANY;
	