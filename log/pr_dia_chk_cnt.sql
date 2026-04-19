CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_dia_chk_cnt(tbl text, rollback boolean DEFAULT false) 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare
    sql text;
    jsn json;
begin
    drop table if exists tmp_dia_chk;

    sql=format($sql$
    create temp table tmp_dia_chk
    with (appendonly=true,orientation=column,compresstype=zstd,compresslevel=3)
    on commit drop as
        select ctl_loading, export_time, srs
            , count(distinct arch_num) = max(arch_cnt) arch_ok
            , max(arch_cnt) arch_cnt
            , sum(cnt) = sum(row_cnt) cnt_ok
            , sum(row_cnt) row_cnt
            , (count(distinct arch_num) = max(arch_cnt)) and (sum(cnt) = sum(row_cnt)) as ok
        from (
            select ctl_loading, export_time, archive_name
                , substring(archive_name from '^NIFI__SF__hrpl_as__([\w|\d]+)_\d{8}_\d{6}_\d+_\d+_\d+\.csv\.zip$')::text srs
                , substring(archive_name from '^NIFI__SF__hrpl_as__[\w|\d]+_\d{8}_\d{6}_(\d+)_\d+_\d+\.csv\.zip$')::int4 arch_num
                , substring(archive_name from '^NIFI__SF__hrpl_as__[\w|\d]+_\d{8}_\d{6}_\d+_(\d+)_\d+\.csv\.zip$')::int4 arch_cnt
                , substring(archive_name from '^NIFI__SF__hrpl_as__[\w|\d]+_\d{8}_\d{6}_\d+_\d+_(\d+)\.csv\.zip$')::int8 row_cnt
                , cnt
            from (
                select ctl_loading, export_time, archive_name, count(1) cnt
                from s_grnplm_vd_hr_edp_dia.%s
                group by 1,2,3
            ) a
        ) a
        group by 1, 2, 3
    DISTRIBUTED randomly;
    $sql$, tbl);
    execute sql;

    select row_to_json(a.*) into jsn from tmp_dia_chk a
    order by a.ctl_loading desc, a.export_time desc, a.srs desc
    limit 1
    ;

    if rollback and not (jsn->>'arch_ok')::bool and not (jsn->>'cnt_ok')::bool then
        raise exception using ERRCODE = 'XX001', MESSAGE = format('Count achive error: %s', jsn::text), DETAIL = jsn::text, HINT = '';
    else
        insert into s_grnplm_vd_hr_edp_srv_wf.tb_swf_dia_log (ts, wf_action, wf_message)
        select now(), tbl, row_to_json(a.*) from tmp_dia_chk a
        order by a.ctl_loading, a.export_time, a.srs
        ;
    end if;

    return jsn::text;

end;
$body$
EXECUTE ON ANY;

COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_dia_chk_cnt(text, boolean) IS 'Проверяет полноту архивов DIA-таблицы по количеству строк и архивов; при rollback=true бросает исключение';
