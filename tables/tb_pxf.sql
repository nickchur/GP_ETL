CREATE TABLE s_grnplm_vd_hr_edp_srv_wf.tb_pxf (
	pxf text not null,
	fld text not null,
	flt text not null,
	inf text not null,
	active boolean not null
)
WITH (appendonly=false)
DISTRIBUTED BY (pxf);