CREATE TABLE s_grnplm_vd_hr_edp_srv_dq.tb_skew_details (
	s_id integer null,
	segment_id integer null,
	cnt bigint null,
	size bigint null
)
WITH (appendonly=true, orientation=column, compresstype=zstd, compresslevel=7)
DISTRIBUTED BY (s_id);