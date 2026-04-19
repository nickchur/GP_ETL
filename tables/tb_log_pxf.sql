CREATE TABLE s_grnplm_vd_hr_edp_srv_wf.tb_log_pxf (
	ts timestamp without time zone null,
	duration interval null,
	pxf text null,
	fld text null,
	flt text null,
	val_old text null,
	val_new text null,
	info text null,
	err text null,
	loops integer null
)
WITH (appendonly=true, orientation=column, compresstype=zstd, compresslevel=3)
DISTRIBUTED BY (pxf);