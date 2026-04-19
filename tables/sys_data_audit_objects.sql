CREATE TABLE s_grnplm_vd_hr_edp_srv_dq.sys_data_audit_objects (
	schema_id oid null,
	object_id oid null,
	schema_name text null,
	object_name text null,
	owner_name text null,
	object_type character varying(150) null,
	description text null,
	source_code text null,
	created_dttm timestamp without time zone null
)
WITH (appendonly=true, compresstype=none, compresslevel=3)
DISTRIBUTED BY (schema_id, object_id);
comment on table s_grnplm_vd_hr_edp_srv_dq.sys_data_audit_objects is 'Актуальная таблица объектов в схемах';
comment on column s_grnplm_vd_hr_edp_srv_dq.sys_data_audit_objects.schema_id is 'Идентификатор схемы';
comment on column s_grnplm_vd_hr_edp_srv_dq.sys_data_audit_objects.object_id is 'Идентификатор схемы';
comment on column s_grnplm_vd_hr_edp_srv_dq.sys_data_audit_objects.schema_name is 'Наименование схемы';
comment on column s_grnplm_vd_hr_edp_srv_dq.sys_data_audit_objects.object_name is 'Наименование объекта';
comment on column s_grnplm_vd_hr_edp_srv_dq.sys_data_audit_objects.owner_name is 'Владелец объекта';
comment on column s_grnplm_vd_hr_edp_srv_dq.sys_data_audit_objects.object_type is 'Тип объекта';
comment on column s_grnplm_vd_hr_edp_srv_dq.sys_data_audit_objects.description is 'Описание объекта';
comment on column s_grnplm_vd_hr_edp_srv_dq.sys_data_audit_objects.source_code is 'Исходный код';
comment on column s_grnplm_vd_hr_edp_srv_dq.sys_data_audit_objects.created_dttm is 'Дата создания';