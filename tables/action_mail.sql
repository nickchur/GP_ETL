CREATE TABLE s_grnplm_vd_hr_edp_srv_dq.action_mail (
	i_pernr integer null,
	fire numeric null,
	cnt numeric null
)
DISTRIBUTED BY (i_pernr);