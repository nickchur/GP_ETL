CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_get_part(_sch text, _tbl text, _lvl integer, _beg date, _end date) 
	RETURNS TABLE(ord integer, prt text, pr_beg text, pr_end text, pr_def boolean)
	LANGUAGE sql
	VOLATILE
as $body$

-- begin
--     return query 
    with part as (
        select *
            , min(dt_beg) over() min_beg
            , max(dt_end) over() max_end
        from (
            select partitionposition ord, substring (partitiontablename from tablename||'(_\w+)') prt
                , partitionrangestart pr_beg, partitionrangeend pr_end, partitionisdefault pr_def
                , substring(partitionrangestart from $s$'(\d{4}-\d{2}-\d{2}).*'::$s$)::date dt_beg
                , substring(partitionrangeend from $s$'(\d{4}-\d{2}-\d{2}).*'::$s$)::date dt_end
            from pg_partitions a 
            where partitionlevel = _lvl and schemaname = _sch and tablename = _tbl
            ) a
    )
    select ord, prt, pr_beg, pr_end, pr_def from part a
    where (
        (dt_end > _beg and dt_beg <= _end)
        or (pr_def = true and (min_beg > _beg or max_end <= _end))
        or min_beg is null
        or _beg is null
        )
    -- union 
    -- select ord, prt, pr_beg, pr_end, pr_def from part b
    -- where min_beg is null or _beg is null
    union 
    select 0 ord, '' prt, null pr_beg, null pr_end, true pr_def 
    where (select 1 from part limit 1) is null
    order by 1;
-- end;

$body$
EXECUTE ON ANY;

COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_get_part(text, text, integer, date, date) IS 'Возвращает список партиций таблицы, пересекающихся с заданным диапазоном дат';
