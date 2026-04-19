CREATE TABLE s_grnplm_vd_hr_edp_srv_wf.tb_exchange_log (
	id bigint not null DEFAULT nextval('s_grnplm_vd_hr_edp_srv_wf.tb_exchange_log_id_seq'::regclass),
	ts timestamp without time zone not null DEFAULT clock_timestamp(),
	wf_name text null,
	wf_key text null,
	wf_data json null
)
WITH (appendonly=true, orientation=column, compresstype=zstd)
DISTRIBUTED RANDOMLY;
comment on column s_grnplm_vd_hr_edp_srv_wf.tb_exchange_log.id is 'Уникальный ID пакета';
comment on column s_grnplm_vd_hr_edp_srv_wf.tb_exchange_log.ts is 'Временная метка записи';
comment on column s_grnplm_vd_hr_edp_srv_wf.tb_exchange_log.wf_name is 'Имя потока/процесса';
comment on column s_grnplm_vd_hr_edp_srv_wf.tb_exchange_log.wf_key is 'Ключевой идентификатор';
comment on column s_grnplm_vd_hr_edp_srv_wf.tb_exchange_log.wf_data is 'JSON с метаданными пакета';