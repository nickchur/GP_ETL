CREATE FUNCTION s_grnplm_vd_hr_edp_srv_dq.pr_ztest_add_except(_object text, _new_date date) 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare
    txt text = '';
    log_id int4;
    _old date[];
    _new date[];
begin
    log_id = s_grnplm_vd_hr_edp_srv_wf.pr_log_action('start', concat('ztest_add_except ', _object));
    begin
        _object = _object::regclass::text;
        
        select  a.z_except into _old from s_grnplm_vd_hr_edp_srv_dq.tb_ztest_config a where object = _object ;
        if not found then
            txt = format('No object %s', _object);
            log_id = s_grnplm_vd_hr_edp_srv_wf.pr_log_action('error', txt, log_id);
            return txt;
        end if;
        -- delete from s_grnplm_vd_hr_edp_srv_dq.tb_ztest_config where object = _object;
        -- _old = coalesce(_old, '{}'::date[]);

        if _new_date = any(_old) then
            with new as (
                update s_grnplm_vd_hr_edp_srv_dq.tb_ztest_config 
                set z_except = new.new_except
                from (
                    select array_agg(z_except order by z_except desc) new_except
                    from (
                        select distinct unnest(_old)::date as z_except
                    ) a
                    where z_except <> _new_date::date
                ) new
                where object = _object
                returning z_except
            )
            select new.z_except into _new from new limit 1;
        else
            with new as (
                update s_grnplm_vd_hr_edp_srv_dq.tb_ztest_config 
                set z_except = new.new_except
                from (
                    select array_agg(z_except order by z_except desc) new_except
                    from (
                        select distinct unnest(_old)::date as z_except
                        union select _new_date::date as z_except
                    ) a
                ) new
                where object = _object
                returning z_except
            )
            select new.z_except into _new from new limit 1;
        end if;

        txt = format('Ok %s %s Old %s', _object, _new::text, _old::text);
        log_id = s_grnplm_vd_hr_edp_srv_wf.pr_log_action('end', txt, log_id);
        return txt;

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

            perform s_grnplm_vd_hr_edp_srv_wf.pr_log_error(log_id, e_txt,e_detail,e_hint,e_context) ; --ЛОГИРОВАНИЕ

            return concat('Error: ', e_txt);
        end;
    end;
end; 
$body$
EXECUTE ON ANY;
	