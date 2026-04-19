CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_employee_hashtags_stat(nn integer DEFAULT 40, rr integer DEFAULT '-2'::integer) 
	RETURNS TABLE(n integer, min bigint, max bigint, sum bigint, diff bigint, dt_cnt bigint, avg bigint, rmin bigint, rmax bigint, rdiff bigint)
	LANGUAGE plpgsql
	VOLATILE
as $body$

begin
	return query 
	select e.*, (e.sum/e.dt_cnt)::int8 avg, round(e.min, rr)::int8 rmin, round(e.max, rr)::int8 rmax, round(e.max, rr)::int8 - round(e.min, rr)::int8 rdiff
	from (
		select d.n::int4, d.min::int8, coalesce (lead(d.min) over(order by d.min), max(d.max) over() + 1)::int8 as max , d.sum::int8
		, coalesce (lead(d.min) over(order by d.min), max(d.max) over() + 1)::int8 - d.min as diff, d.dt_cnt::int8
		from (
			select c.n, min(c.min), max(c.max), sum(c.count)::int8, avg(c.dt_cnt)::int8 dt_cnt
			from (
				select b.gp_segment_id, b.n, min(b.id), max(b.id), count(1), count(distinct dt) dt_cnt
				from (
					select a.gp_segment_id, a.nsi_id::int as id, ntile(nn) over(partition by a.gp_segment_id order by a.nsi_id) n, a.row_actual_to_dt as dt
					from s_grnplm_vd_hr_edp_stg.tb_smd_employee_hashtags_new a
				) b
				group by 1,2
			) c
			group by 1
		) d
	) e
	order by min;
end; 
$body$
EXECUTE ON ANY;
	