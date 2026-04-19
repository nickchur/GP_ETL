CREATE TABLE s_grnplm_vd_hr_edp_srv_dq.fg_an2 (
	o_i_pernr integer null,
	i_pernr integer null,
	report_date date null,
	boss_i_pernr integer null,
	boss_fio character varying(500) null,
	ossbpi integer null,
	ossbf character varying(600) null,
	fgbip integer null,
	fgbf character varying(600) null
)
DISTRIBUTED BY (o_i_pernr);