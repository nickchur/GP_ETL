CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_log_skew_hist(tb_name text, tb_size bigint, p_calc_size boolean default null)
	RETURNS text
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
as $body$

declare
    size_txt text;
    gp_seg text;
    tt timestamp := clock_timestamp();
    _last json;
    exe text = '';
begin
    set search_path to s_grnplm_vd_hr_edp_srv_wf;

    if p_calc_size is null then
        -- авто: считать если столбцов <= 300 и размер в допустимом диапазоне
        if (select max(pa.attnum) from pg_catalog.pg_attribute pa where pa.attrelid = tb_name::regclass::oid) <= 300
                and tb_size between 1 and replace('100 000 000 000', ' ', '')::int8
        then
            size_txt = 'sum(pg_column_size(a.*))';
        else
            size_txt = 'null::int8';
        end if;
    elsif p_calc_size then
        size_txt = 'sum(pg_column_size(a.*))';
    else
        size_txt = 'null::int8';
    end if;

    if (select policytype from pg_catalog.gp_distribution_policy a where a.localoid = tb_name::regclass::oid) = 'r' then
        gp_seg = 'null::int4';
    else
        gp_seg = 'gp_segment_id';
    end if;       

    drop TABLE if exists tmp_skew;
    exe = format($sql$
        CREATE temp TABLE tmp_skew
        WITH (appendonly=true, orientation=row, compresstype=zstd, compresslevel=3)
        on commit drop as
            select count(segment_id)::int4 as segments
                , sum(cnt)::int8 as sum
                , min(cnt)::int8 as min
                , max(cnt)::int8 as max
                , avg(cnt)::int8 as avg
                , stddev(cnt)::int8 std
                , sum(data_size)::int8 as data_size
            from ( select %2$s as segment_id, count(1) as cnt, %3$s as data_size from %1$s as a group by 1) a
        DISTRIBUTED randomly;
    $sql$, tb_name, gp_seg, size_txt);
    execute exe;

    with add as (
        insert into s_grnplm_vd_hr_edp_srv_wf.tb_log_skew (
            ts, tbl, skew
             , segments, sum, min, max, avg, std, data_size
             , distributedby, tbl_size, duration, options, storage
            )
        select now()
            , tb_name::regclass::text as tbl
            , 1.0 * std /  nullif(avg, 0) skew
            , a.*
            , pg_get_table_distributedby(tb_name::regclass::oid) distributedby
            , tb_size as tbl_size
            , clock_timestamp() - tt as duration
            , c.reloptions::text as options
            , c.relstorage as storage
        from tmp_skew a
        left join pg_class c on c.oid = tb_name::regclass::oid
        returning *
    )
    select row_to_json(add.*) into _last from add limit 1;
    
    return format('OK skew %s', _last::text);

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

        perform pr_Log_error(0, e_txt, e_detail, e_hint, e_context) ;
        return e_txt;
    end;
end;
$body$
EXECUTE ON ANY;

COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_log_skew_hist(text, bigint, boolean) IS 'Вычисляет и сохраняет метрики перекоса (min/max/avg/std по сегментам) таблицы в tb_log_skew. p_calc_size: null=авто (<=300 колонок и размер<=100GB), true=всегда считать data_size, false=не считать';
