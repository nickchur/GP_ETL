CREATE TABLE s_grnplm_vd_hr_edp_srv_wf.tb_swf_hist (
	now timestamp without time zone null,
	wf_id integer not null,
	wf_order integer not null,
	wf_name text not null,
	wf_exec text not null,
	wf_beg timestamp without time zone not null,
	wf_interval interval not null,
	wf_end timestamp without time zone null,
	wf_relations text[] null,
	wf_waits text[] null,
	wf_last timestamp without time zone null,
	wf_duration interval null,
	wf_swf text null,
	wf_reselt json null,
	wf_expire interval not null
)
WITH (appendonly=true, orientation=column, compresstype=zstd, compresslevel=3)
DISTRIBUTED BY (wf_name);