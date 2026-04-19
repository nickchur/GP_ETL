CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_add_partitions(mode text DEFAULT 'nowait'::text, dt date DEFAULT NULL::date) 
	RETURNS text
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
as $body$


declare
    lid int4;
    app text;
    txt text;
    exe text;
    chk json;
    rec record;
    k int = 1;
    nbeg text;
    nend text;
    npos int4;
    err int4 = 0;
    prt int4 = 0;
begin
    set search_path to s_grnplm_vd_hr_edp_srv_wf;
    execute 'show application_name' into app;
    lid = pr_Log_start('TEST_');
    
    dt = coalesce(dt, (current_date + '3 month'::interval)::date);

    raise info '%', clock_timestamp();
    begin
        for rec in (
            select a.schemaname, a.tablename, a.max_interval, a.max_rank
                , substring(b.partitionrangeend from $s$\'.*\'::(.+)$s$) p_type
                , substring(b.partitionrangestart from $s$\'(.*)\'::$s$) p_beg
                , case b.partitionstartinclusive when true then '' else 'EXCLUSIVE' end p_beg_inc
                , substring(b.partitionrangeend from $s$\'(.*)\'::$s$) p_end
                , case b.partitionendinclusive when true then 'INCLUSIVE' else '' end p_end_inc
            --    , b.partitionrangeend, b.partitionendinclusive
            --    , c.partitionname p_default
            from (
                select a.schemaname, a.tablename
                    , max(a.partitionrangeend) max_end
                    , max(a.partitioneveryclause) max_interval
                    , max(a.partitionisdefault::int)::bool has_def
                    , max(a.partitionlevel) max_level
                    , max(a.partitionrank) max_rank
                from pg_partitions a
                where a.schemaname like 's_grnplm_vd_hr_edp_%'
					and a.tablename != '_tempti_tb_oss_sep'
                and a.partitiontype = 'range'
                group by 1,2 
            ) a 
            join pg_partitions b 
                on a.schemaname = b.schemaname 
                and a.tablename = b.tablename
                and a.max_end = b.partitionrangeend
            --join pg_partitions c
            --    on a.schemaname = c.schemaname 
            --    and a.tablename = c.tablename
            --    and c.partitionisdefault
            where a.has_def and a.max_level = 0 
                and max_interval like '%::interval'
                and substring(b.partitionrangeend from $s$\'(.*)\'::$s$) < dt::text
            order by 1, 2
        ) loop
	        begin
--		        execute format('set application_name = %L', app||'>'||rec.tablename);
		        execute format('set application_name = %L', 'add_part>'||rec.tablename);
	
                exe = chr(10)||format($sql$lock table %I.%I in ACCESS EXCLUSIVE MODE NOWAIT;$sql$, rec.schemaname, rec.tablename);
                if mode in ('nowait', 'sql') then
                	raise info '%', exe;
	            end if;
	            if mode = 'nowait' then
	                execute exe;
	            end if;
	           
	            nbeg = rec.p_beg;
	            nend = rec.p_end;
	            npos = rec.max_rank + 1;
	            begin
	                loop
	                    npos = npos + 1;
	
	                    exe = format('select (%L::%s + %s)::%s', nbeg, rec.p_type, rec.max_interval, rec.p_type);
	--                    raise info '%', exe;
	                    execute exe into nbeg;
	                    
	                    exe = format('select (%L::%s + %s)::%s', nend, rec.p_type, rec.max_interval, rec.p_type);
	--                    raise info '%', exe;
	                    execute exe into nend;
	                    
	                    exit when nbeg > dt::text;
	
	                    exe = format($sql$alter table %I.%I split default partition start(%L) %s END(%L) %s INTO (partition p1_%s, default partition);$sql$
	                        , rec.schemaname, rec.tablename
	                        , nbeg, rec.p_beg_inc, nend, rec.p_end_inc, npos
	                    );
	                    raise info '%', exe;
	                    if mode <> 'sql' then
	                    	execute exe;
	                    end if;
	    
	                end loop;
	                prt = prt + 1;
	
	            exception when OTHERS then
	                raise info '%', format('Error: %s (%s)', sqlerrm, sqlstate);
	                err = err + 1;
	            end;
	
	        exception when OTHERS then
	            raise info '%', format('Error: %s (%s)', sqlerrm, sqlstate);
	            err = err + 1;
	        end;
        end loop;
        
--        if coalesce(rc, 0) = 0 then
--            txt = format('No data in %s', wf);
--            lid = pr_Log_error(lid, txt) ;
--            return txt;
--        end if;
        
        execute format('set application_name = %L', app);
        lid = pr_Log_end(lid, '');
        return format('Ok %s Err %s', prt, err);
    
    exception when OTHERS then
        declare e_detail text; e_hint text; e_context text;
        begin
            get stacked diagnostics e_detail = PG_EXCEPTION_DETAIL, e_hint = PG_EXCEPTION_HINT, e_context = PG_EXCEPTION_CONTEXT;
            
            if sqlstate = 'XX001' then
                raise exception using ERRCODE = sqlstate, MESSAGE = sqlerrm, DETAIL = e_detail, HINT = e_hint;
            end if;
            
            if split_part(e_context, 'statement', 1) like '% at EXECUTE %' then
                e_detail = exe;
                e_context = replace(e_context, exe, '...');
            end if;
            
            lid = pr_log_error(lid, sqlerrm, e_detail, e_hint, e_context) ;
            
            return format('Error: %s (%s)', sqlerrm, sqlstate);
        end;
    end;
end;


$body$
EXECUTE ON ANY;

COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_add_partitions(text, date) IS 'Автоматически добавляет недостающие партиции во все партиционированные таблицы схем s_grnplm_vd_hr_edp_*';
