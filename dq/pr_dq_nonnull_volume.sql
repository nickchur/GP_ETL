CREATE FUNCTION s_grnplm_vd_hr_edp_srv_dq.pr_dq_nonnull_volume(tbl text, params json) 
	RETURNS json
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
as $body$

declare
    metric text = 'nonnull_volume';
    dt_type text =  coalesce(params->>'type', 'month');
    fld text = (params->>'fn')::text;
    rdn text = (params->>'rd')::text;
    
    min numeric;
    max numeric;
    
    dtp date;
    lid int4;
    exe text;
    jsn json;
begin
    lid = s_grnplm_vd_hr_edp_srv_dq.pr_log_dq(metric, json_build_object('tbl', tbl, 'params', params));
    begin
        min = (params->>'min')::numeric;
        max = (params->>'max')::numeric;
        
        dtp = current_date + coalesce((params->>'offset')::int4, - 1);
        dtp = date_trunc(dt_type, dtp + 1)::date - 1;
        
        exe = format($sql$
            with chk as (
                select %1$s::date rd, count(%2$s) cnt
                from s_grnplm_vd_hr_edp_%3$s 
                where %1$s::date = %4$L::date
                    or %1$s::date = (%4$L::date + 1  - '1 %5$s'::interval)::date - 1
                    or %1$s::date = (%4$L::date + 1  - '2 %5$s'::interval)::date - 1
                group by 1
            )
            select row_to_json(a) 
            from (
                -- select case when coalesce(a.ratio_3, a.ratio_1, 1.0) between %6$s and %7$s then 1 else 0 end as res
                select case when coalesce(a.ratio_3, a.ratio_1, null) between %6$s and %7$s then 1 else 0 end as res
                    , a.*
                    -- , (clock_timestamp() - %8$L::timestamp)::interval(0) dur
                from (
                    select rd as dt 
                        -- , %5$L as metric
                        , cnt as val2chk
                        , nullif((cnt::numeric / nullif((select cnt from chk where rd = (%4$L::date + 1  - '1 %5$s'::interval)::date - 1), 0)), 0)::float4 as ratio_1
                        , nullif((cnt::numeric / nullif((select avg(cnt) from chk where rd < %4$L::date), 0)), 0)::float4 as ratio_3
                    from chk
                    where rd = %4$L::date 
                    -- limit 1
                ) a
            ) a
        $sql$, rdn, fld, tbl, dtp, dt_type, min, max, clock_timestamp());
        execute exe into jsn;
        jsn = coalesce(jsn, json_build_object('res', -1, 'error', 'Empty data', 'dt', dtp));
        
        lid = s_grnplm_vd_hr_edp_srv_dq.pr_log_dq(case (jsn->>'res')::int when 0 then 'False' when 1 then 'True' else 'Error' end, jsn, lid);
        return jsn;

    exception when OTHERS then
        -- raise notice '%', exe;
        -- perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_error(0, sqlerrm, exe, null, null);
        jsn = json_build_object('res', -99, 'error', sqlerrm);
        lid = s_grnplm_vd_hr_edp_srv_dq.pr_log_dq('error', jsn, lid);
        return jsn;
    end;
end;

$body$
EXECUTE ON ANY;
	
COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_dq.pr_dq_nonnull_volume(text, json) IS 'Рассчитывает метрики заполненности и объёма таблицы по DQ-правилам и возвращает результат в JSON';
