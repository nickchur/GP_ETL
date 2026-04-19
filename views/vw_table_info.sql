CREATE VIEW s_grnplm_vd_hr_edp_srv_dq.vw_table_info AS
 SELECT (((n.nspname)::text || '.'::text) || (c.relname)::text) AS full_table_name,
    (n.nspname)::text AS schema_name,
    pg_size_pretty(pg_relation_size((c.oid)::regclass)) AS pretty_table_size,
    pg_relation_size((c.oid)::regclass) AS byte_table_size
   FROM (pg_class c
     LEFT JOIN pg_namespace n ON ((n.oid = c.relnamespace)))
  WHERE ((1 = 1) AND (n.nspname ~~ ('s_grnplm_vd_hr_edp%'::name)::text))
  ORDER BY pg_total_relation_size((c.oid)::regclass) DESC;

