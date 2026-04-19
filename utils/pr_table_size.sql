CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_table_size(tb_name name) 
	RETURNS bigint
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare
   tb_oid oid;
    size int8;
begin
--    tb_oid = s_grnplm_vd_hr_edp_srv_wf.try_cast2regclass(tb_name)::oid;
--    return (
--        with recursive h(inhrelid, inhparent, inhseqno, size) as (
--            select tb_oid inhrelid, null::oid inhparent, 0::int4 inhseqno, pg_total_relation_size(tb_oid)::int8 size
--            union 
--            select a.*, pg_total_relation_size(a.inhrelid)::int8 size from pg_catalog.pg_inherits a, h
--            where a.inhparent = h.inhrelid
--        )
--        select sum(size)::int8 from h);
    size = (
        with rel as (
            select pg_total_relation_size(tb_name::regclass)::int8 as tsize
            union all
            select pg_total_relation_size(format('%I.%I', partitionschemaname, partitiontablename)::regclass)::int8 as tsize
            from pg_partitions a
            where schemaname like 's_grnplm_vd_hr_edp_%'
                and format('%I.%I', schemaname, tablename)::regclass = tb_name::regclass
        ) 
        select sum(tsize)::int8 from rel
    );

    return size;
        
exception when OTHERS then
    return null;
end;
$body$
EXECUTE ON ANY;
	