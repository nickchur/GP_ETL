CREATE FUNCTION s_grnplm_vd_hr_edp_srv_dq.fn_dq_delta_perc_prev(table_name character varying, field_name character varying) 
	RETURNS numeric
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare
	delta_perc numeric(18,4) default 0;
begin
	execute 'with ctl as
	(
		select case when D.rn=1 then ''last'' else ''prev'' end as rn
			, P.'||field_name||'
			, count(*) as cnt
		from '||table_name||' as P
		inner join (
			select *
			from
			(
				select '||field_name||', row_number() over(order by '||field_name||' desc) as rn
				from '||table_name||'
				where '||field_name||' is not null
				group by '||field_name||'
			) as T
			where T.rn <= 2
		) as D
		on 1=1
			and D.'||field_name||'=P.'||field_name||'
		group by 1, 2
	)
	select ((v_last::numeric(18,8)-v_prev::numeric(18,8))/v_prev::numeric(18,8))::numeric(18,8)*100 as delta
	from (	select (select cnt from ctl where rn=''last'') as v_last,
					(select cnt from ctl where rn=''prev'') as v_prev
		) as T'
		into delta_perc
	using table_name;
	return delta_perc;
end; 
$body$
EXECUTE ON ANY;
	