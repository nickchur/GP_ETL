CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_log_fld_stat(tbl name, lmt bigint DEFAULT 1000000) 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$
 
declare 
    -- 2025-06-03
    -- tbl name default 's_grnplm_vd_hr_edp_dia.isu_data_task';
    fld name;
    flt name;
    flf name;
    num int4;
    rw record;
    exe text;
    min_max text;
    flg bool default true;
    m_txt text; 
    e_detail text;
    e_hint text;
    e_context text;
    dt timestamp;
    ret record;
begin 
    set search_path to s_grnplm_vd_hr_edp_srv_wf;
    drop table if exists tmp_struct;
    -- truncate tmp_struct ;

    for num, fld, flt, flf in (
        select pa.attnum, pa.attname , format_type(pa.atttypid, null), format_type(pa.atttypid, pa.atttypmod) --, *
        from pg_catalog.pg_attribute pa where pa.attrelid = tbl::regclass::oid and pa.attnum > 0 order by pa.attnum
    ) loop

        if (flt in ('bytea','jsonb','json','balance_item','finrez_item','xid','pg_lsn','inet','bit','cidr','polygon','xml','aclitem')) then
            min_max = ', min(length(fl::text))::text min, max(length(fl::text))::text max';
        elseif flt in ('boolean','uuid') then
            min_max = ', min(fl::text) min, max(fl::text) max';
        elseif (flt similar to '\w+\[\]') then
            -- min_max = ', min(array_dims(fl)) min, max(array_dims(fl)) max';
            min_max = ', min(array_length(fl,1)) min, max(array_length(fl,1)) max';
        else 
            min_max = ', min(fl)::text min, max(fl)::text max';
        end if;

        -- sql3 = format($$select '%1$s' tbl, %2$s fld_num, '%3$s'::text fld_name, '%4$s'::text fld_type
        -- , '%6$s'::timestamp dt, '%7$s'::timestamp as dtf$$, tbl, num::text, fld, flf, now(), clock_timestamp());


        exe = format($sql$
                select 
                    -- now() ts
                    -- , '%1$s'::text tbl_name
                    '%2$s'::text fld_name
                    ,  %3$s fld_num
                    , pr_get_format_type('%1$s', '%2$s') fld_type
                    , (count(1) = count(fl)) is_not_null
                    , (count(1) = sum(cnt)) is_uniq
                    %4$s
                    , count(1) dist_cnt --, count(distinct fl) cnt_distinct
                    , sum(null_cnt)::int8 null_cnt
                    , sum(cnt)::int8 cnt
                    , min(cnt) min_cnt
                    , max(cnt) max_cnt
                    , avg(cnt)::int8 avg_cnt
                    , min(pg_column_size(fl)) min_size
                    , avg(pg_column_size(fl))::int avg_size
                    , max(pg_column_size(fl)) max_size
                    , min(length(fl::text)) min_len
                    , avg(length(fl::text))::int avg_len
                    , stddev(length(fl::text))::int std_len
                    , max(length(fl::text)) max_len
                    , sum((try_cast2bool(fl::text) is not null)::int) cnt_bool
                    , sum((try_cast2timestamp(fl::text) is not null)::int) cnt_dt
                    , sum((try_cast2regclass(fl::text) is not null)::int) cnt_regclass
                    , sum((try_cast2int(fl::text) is not null)::int) cnt_int
                    , sum((try_cast2float(fl::text) is not null)::int) cnt_float
                    , sum((try_cast2uuid(fl::text) is not null)::int) cnt_uuid
                    -- , sum((lower(fl::text)  similar to '[0-9a-f]+')::int) cnt_hex
                    , sum((try_cast2array(fl::text) is not null)::int) cnt_array
                    , sum((try_cast2jsonb(fl::text) is not null)::int) cnt_json
                from (
                    select fl, count(1) cnt, count(1) - count(fl) null_cnt 
                    from (
                        select %2$s fl from %1$s limit %5$s000
                    ) a group by 1 limit %5$s
                ) a
        $sql$, tbl, fld, num::text, min_max, lmt::text);

        if flg then 
            raise info '%',exe;
            execute format($sql$
                create temp table tmp_struct 
                with (appendonly=true,orientation=row,compresstype=zstd,compresslevel=3)
                on commit drop as
                %s
                limit 0 
                DISTRIBUTED randomly
            $sql$, exe);
            flg = false;
        end if;

        raise info '% % %(%) %', clock_timestamp(), num, fld, flf, min_max;

        begin
            execute format($sql$
                insert into tmp_struct 
                %s
                returning *
            $sql$, exe) into ret;
            raise info '% %', clock_timestamp(), ret;

        exception when OTHERS then
            get stacked diagnostics exe = PG_EXCEPTION_CONTEXT;
            raise exception using ERRCODE = sqlstate, MESSAGE = concat(sqlerrm,' (',sqlstate,')')
                , DETAIL = coalesce(split_part(exe, 'statement\n', 1),''), HINT = pr_get_func(exe);
        end; 
    end loop;

    insert into tb_log_fld_stat
    select 
        now() ts
        , tbl::text tbl_name
        , fld_num
        , fld_name
        , row_to_json(a.*)
    from (
        select case 
            when null_cnt = cnt then 'null'
            when dist_cnt = cnt_bool + (1-is_not_null::int) then 'bool'
            when dist_cnt = 1 and  is_not_null then 'one'
            when dist_cnt = 2 and  not is_not_null then 'one + null'
            when dist_cnt = 2 and  is_not_null then 'two'
            when dist_cnt = 3 and  not is_not_null then 'two + null'
            when dist_cnt = cnt_dt + (1-is_not_null::int) then 'datetime'
            when dist_cnt = cnt_regclass + (1-is_not_null::int) then 'regclass'
            when dist_cnt = cnt_int + (1-is_not_null::int) then 'int'
            when dist_cnt = cnt_float + (1-is_not_null::int) then 'float'
            when dist_cnt = cnt_uuid + (1-is_not_null::int) then 'uuid'
            -- when dist_cnt = cnt_hex + (1-is_not_null::int) then 'hex'
            when dist_cnt = cnt_array + (1-is_not_null::int) then 'array'
            when dist_cnt = cnt_json + (1-is_not_null::int) then 'json'
            else null end fld_stat
            , * 
        from tmp_struct
    ) a
    ;

    return 'Ok';
    
exception when OTHERS then
    get stacked diagnostics exe = PG_EXCEPTION_CONTEXT;
    raise exception using ERRCODE = sqlstate, MESSAGE = concat(sqlerrm,' (',sqlstate,')')
        , DETAIL = coalesce(split_part(exe, 'statement\n', 1),''), HINT = pr_get_func(exe);
end; 
$body$
EXECUTE ON ANY;
	