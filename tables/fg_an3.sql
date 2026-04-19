CREATE TABLE s_grnplm_vd_hr_edp_srv_dq.fg_an3 (
	i_pernr integer null,
	boss_i_pernr integer null,
	boss_fio character varying(500) null,
	lin_i_pernr integer null,
	lin_boss_fio character varying(600) null
)
DISTRIBUTED BY (i_pernr);