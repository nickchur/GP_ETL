CREATE TABLE s_grnplm_vd_hr_edp_srv_wf.tb_swf_status (
	swf_name text not null,
	last_beg timestamp without time zone null,
	last_end timestamp without time zone null,
	last_message json null
)
WITH (appendonly=false)
DISTRIBUTED BY (swf_name);