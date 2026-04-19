CREATE TABLE s_grnplm_vd_hr_edp_srv_dq.sys_dim_hr_edp_schema (
	schema_id oid null,
	schema_name text null,
	schema_owner oid null
)
WITH (appendonly=true, compresstype=none, compresslevel=3)
DISTRIBUTED REPLICATED;
comment on table s_grnplm_vd_hr_edp_srv_dq.sys_dim_hr_edp_schema is 'Справочник схем';
comment on column s_grnplm_vd_hr_edp_srv_dq.sys_dim_hr_edp_schema.schema_id is 'Идентификатор схемы';
comment on column s_grnplm_vd_hr_edp_srv_dq.sys_dim_hr_edp_schema.schema_name is 'Наименование схемы';
comment on column s_grnplm_vd_hr_edp_srv_dq.sys_dim_hr_edp_schema.schema_owner is 'Владелец схемы';