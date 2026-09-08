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

COMMENT ON TABLE s_grnplm_vd_hr_edp_srv_wf.tb_ctl_alerts IS 'Заведённые алерты Пакетной выгрузки. v1.0';
COMMENT ON COLUMN s_grnplm_vd_hr_edp_srv_wf.tb_ctl_alerts.ts IS 'Когда алерт заведён. В пределах одного прогона значения разные (clock_timestamp построчно), иначе порядок в отчёте был бы произвольным';
COMMENT ON COLUMN s_grnplm_vd_hr_edp_srv_wf.tb_ctl_alerts.wf_id IS 'Идентификатор потока в CTL; ключ распределения';
COMMENT ON COLUMN s_grnplm_vd_hr_edp_srv_wf.tb_ctl_alerts.wf_name IS 'Имя потока в CTL';
COMMENT ON COLUMN s_grnplm_vd_hr_edp_srv_wf.tb_ctl_alerts.alert_grp IS 'Группа из параметра потока wf_alert_group; по ней фильтрует аргумент функции';
COMMENT ON COLUMN s_grnplm_vd_hr_edp_srv_wf.tb_ctl_alerts.alert_key IS 'Вид правила и его параметры; вторая часть ключа дедупликации';
COMMENT ON COLUMN s_grnplm_vd_hr_edp_srv_wf.tb_ctl_alerts.period_ts IS 'Начало периода правила; третья часть ключа дедупликации — следующий период даёт новую строку и новую реакцию';
COMMENT ON COLUMN s_grnplm_vd_hr_edp_srv_wf.tb_ctl_alerts.reacted_ts IS 'Когда по алерту вернули ошибку в CTL; NULL — ещё не реагировали';
COMMENT ON COLUMN s_grnplm_vd_hr_edp_srv_wf.tb_ctl_alerts.res IS 'Код результата, с которым алерт заведён; сейчас всегда -6';
COMMENT ON COLUMN s_grnplm_vd_hr_edp_srv_wf.tb_ctl_alerts.msg IS 'Причина алерта человеческим языком: какой даты не хватает или как давно нет статистики';
COMMENT ON COLUMN s_grnplm_vd_hr_edp_srv_wf.tb_ctl_alerts.jsn IS 'Правило и замер целиком — чтобы разобрать срабатывание, не воспроизводя его';
