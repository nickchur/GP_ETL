CREATE TABLE s_grnplm_vd_hr_edp_srv_wf.tb_vda (
	vda_name text not null,
	actual boolean not null,
	main text null,
	workflows text[] null,
	sub_wfs text[] null,
	z_except date[] null
)
WITH (appendonly=false)
DISTRIBUTED REPLICATED;