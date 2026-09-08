-- E360-6367. Заведённые алерты потоков CTL.
-- 2026-09-08 10:24 MSK, v1.1, Чуркин Николай
--
-- Распределение случайное: значений wf_id десятки, а сегментов в бою 200 - хэш по
-- нему сложил бы всю таблицу на пятую часть узлов.
--
-- Отметки реакции нет намеренно. Реакция - это возврат res = -6 из того же вызова,
-- который завёл строку, так что отдельная колонка всегда повторяла бы ts. Заодно из
-- функции ушёл update: в GP он держит строки до конца транзакции, а тут хватает insert.

CREATE TABLE s_grnplm_vd_hr_edp_srv_wf.tb_ctl_alerts (
	ts timestamp without time zone not null DEFAULT clock_timestamp(),
	wf_id bigint null,
	wf_name text null,
	alert_grp text null,
	alert_key text null,
	period_ts timestamp without time zone null,
	res integer null,
	msg text null,
	jsn json null
)
WITH (appendonly=true, orientation=column, compresstype=zstd)
DISTRIBUTED RANDOMLY;

COMMENT ON TABLE s_grnplm_vd_hr_edp_srv_wf.tb_ctl_alerts IS 'Заведённые алерты потоков CTL. v1.1, 2026-09-08';
COMMENT ON COLUMN s_grnplm_vd_hr_edp_srv_wf.tb_ctl_alerts.ts IS 'Время заведения алерта';
COMMENT ON COLUMN s_grnplm_vd_hr_edp_srv_wf.tb_ctl_alerts.wf_id IS 'Идентификатор потока в CTL';
COMMENT ON COLUMN s_grnplm_vd_hr_edp_srv_wf.tb_ctl_alerts.wf_name IS 'Имя потока';
COMMENT ON COLUMN s_grnplm_vd_hr_edp_srv_wf.tb_ctl_alerts.alert_grp IS 'Группа алерта';
COMMENT ON COLUMN s_grnplm_vd_hr_edp_srv_wf.tb_ctl_alerts.alert_key IS 'Ключ правила';
COMMENT ON COLUMN s_grnplm_vd_hr_edp_srv_wf.tb_ctl_alerts.period_ts IS 'Начало периода правила';
COMMENT ON COLUMN s_grnplm_vd_hr_edp_srv_wf.tb_ctl_alerts.res IS 'Код результата';
COMMENT ON COLUMN s_grnplm_vd_hr_edp_srv_wf.tb_ctl_alerts.msg IS 'Причина алерта';
COMMENT ON COLUMN s_grnplm_vd_hr_edp_srv_wf.tb_ctl_alerts.jsn IS 'Правило и замер';
