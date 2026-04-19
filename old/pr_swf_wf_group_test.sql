CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_swf_wf_group_test(fnc text[], rel text[]) 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$


declare
    res  int[];
    mif  bool default true;
    err  bool default true;
    rif int[];
    k int;
    i int;
    ret text default '';
    log_id int4 default null;
    m_txt text;
    sql text;
    app text;
    func text;
begin
    -- Set the transaction isolation level to READ COMMITTED to avoid long-lasting locks
    set transaction isolation level read committed;

    set search_path to s_grnplm_vd_hr_edp_srv_wf;
    execute 'show application_name' into app;
    begin
        for k  in 1..array_length(fnc,1) loop
            if (mif) then
                -- Construct function call more safely and efficiently
                func = (case when left(lower(trim(fnc[k])),3) = 'pr_' then fnc[k] else 'pr_'||fnc[k] end)||'()';
            
                -- Execute the function and store the result
                execute 'select '||func into m_txt;  
                
                -- Simplify result checking
                res[k] = case when m_txt like 'ok%' then 1 when m_txt like 'no%' then 0 else -1 end;
            else 
                m_txt = 'WF skipped';
                res[k] = 0;
            end if;
            ret = ret || E'
<' || fnc[k] || ': ' || m_txt;
       
            -- Dependency handling
            rif = rel[k]::int[];
            if array_length(rif,1) >= 1 then
                mif = false;
                err = false;
                foreach i in array rif loop
                    mif = mif or (res[rif[i]] =  1);
                    err = err or (res[rif[i]] = -1);
                end loop;
            else 
                mif = true;
            end if;
        end loop;

        -- Final result aggregation
        mif = true;
        err = false;
        foreach i in array fnc loop
            mif = mif and (res[i] =  1);
            err = err or  (res[i] = -1);
        end loop;

        -- Result determination
        if err then
            return 'Error'||ret;
        elsif mif then
            return 'Ok'||ret;
        else 
            return 'No'||ret;
        end if;

    exception when OTHERS then
        -- Improved error handling
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
            
            log_id = pr_Log_error(log_id,e_txt,e_detail,e_hint,e_context) ;
            return 'Error '||e_txt||ret;
        end;
    end;
end; 

$body$
EXECUTE ON ANY;
	