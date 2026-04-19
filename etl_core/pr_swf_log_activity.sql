CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_swf_log_activity(patern text DEFAULT '%'::text, timeout interval DEFAULT NULL::interval) 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare 
	act_rec record;
	act_res bool;
	log_id int4;
	rc int;
begin
	patern = coalesce(patern, '% s_grnplm_vd_hr_edp_srv_wf.pr_swf_start_all(%');

--	insert into tb_swf_stat_activity select now(),*  from  pg_stat_activity  where usename = user;
	rc = 0;

	for act_rec in (select (now()-xact_start) duration, * from pg_stat_activity 
					where usename = user and state = 'active'
						and lower(query) like patern
						order by 1 desc) loop
		rc = rc + 1;
		raise info '% %', rc , act_rec; 
		if timeout is not null and (now() - act_rec.xact_start) >= timeout then
			log_id = pr_swf_log_action(now()::timestamp::text, null, json_build_object('xact', act_rec.xact_start, 'pid', act_rec.pid, 'state', act_rec.state, 'lwf', lwf));
			act_res = pg_cancel_backend(act_rec.pid);
			log_id = pr_swf_log_action('cancel', null, json_build_object('reselt', act_res::int, 'duration', (now() - act_rec.xact_start),'msg', act_rec.query), log_id);

			return act_rec::text;
		end if;
	end loop;
	return 'Ok '||rc::text;
end; 
$body$
EXECUTE ON ANY;
	

COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_swf_log_activity(text, interval) IS 'Возвращает список активных сессий БД, ожидающих указанный паттерн запроса, с возможным ожиданием';
