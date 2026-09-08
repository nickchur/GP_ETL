-- E360-6367. Заведённые алерты Пакетной выгрузки и отметки реакции.
-- 2026-09-07 18:34 MSK, v1.0, Чуркин Николай

CREATE TABLE s_grnplm_vd_hr_edp_srv_wf.tb_ctl_alerts (
	ts timestamp without time zone not null DEFAULT clock_timestamp(),
	wf_id bigint null,
	wf_name text null,
	alert_grp text null,
	alert_key text null,
	period_ts timestamp without time zone null,
	reacted_ts timestamp without time zone null,
	res integer null,
	msg text null,
	jsn json null
)
WITH (appendonly=false)
DISTRIBUTED BY (wf_id);

COMMENT ON TABLE s_grnplm_vd_hr_edp_srv_wf.tb_ctl_alerts IS 'Заведённые алерты Пакетной выгрузки. v1.0, 2026-09-07';
COMMENT ON COLUMN s_grnplm_vd_hr_edp_srv_wf.tb_ctl_alerts.ts IS 'Время заведения алерта';
COMMENT ON COLUMN s_grnplm_vd_hr_edp_srv_wf.tb_ctl_alerts.wf_id IS 'Идентификатор потока в CTL';
COMMENT ON COLUMN s_grnplm_vd_hr_edp_srv_wf.tb_ctl_alerts.wf_name IS 'Имя потока';
COMMENT ON COLUMN s_grnplm_vd_hr_edp_srv_wf.tb_ctl_alerts.alert_grp IS 'Группа алерта';
COMMENT ON COLUMN s_grnplm_vd_hr_edp_srv_wf.tb_ctl_alerts.alert_key IS 'Ключ правила';
COMMENT ON COLUMN s_grnplm_vd_hr_edp_srv_wf.tb_ctl_alerts.period_ts IS 'Начало периода правила';
COMMENT ON COLUMN s_grnplm_vd_hr_edp_srv_wf.tb_ctl_alerts.reacted_ts IS 'Время реакции';
COMMENT ON COLUMN s_grnplm_vd_hr_edp_srv_wf.tb_ctl_alerts.res IS 'Код результата';
COMMENT ON COLUMN s_grnplm_vd_hr_edp_srv_wf.tb_ctl_alerts.msg IS 'Причина алерта';
COMMENT ON COLUMN s_grnplm_vd_hr_edp_srv_wf.tb_ctl_alerts.jsn IS 'Правило и замер';
