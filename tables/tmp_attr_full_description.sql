CREATE TABLE s_grnplm_vd_hr_edp_srv_dq.tmp_attr_full_description (
	object_id oid null,
	full_text_column text null,
	full_text_comment text null
)
WITH (appendonly=true, compresstype=none, compresslevel=3)
DISTRIBUTED BY (object_id);