CREATE TABLE s_grnplm_vd_hr_edp_srv_wf.tb_log_fld_stat (
	ts timestamp without time zone not null,
	tbl_name text not null,
	fld_num integer not null,
	fld_name text not null,
	data json null
)
WITH (appendonly=true, orientation=column, compresstype=zstd, compresslevel=3)
DISTRIBUTED RANDOMLY;