CREATE FUNCTION s_grnplm_vd_hr_edp_srv_dq.fn_dq_calc_dm2() 
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare
begin
       DELETE FROM s_grnplm_vd_hr_edp_srv_dq.tb_dq_check_status where 1 = 1;
             insert into s_grnplm_vd_hr_edp_srv_dq.tb_dq_check_status (
             select
             tdr.rule_name, rule_table_name, rule_field_name,
               rule_type_cd,  rule_upper_value control_value, actual_value,
               is_passed,
               load_dt
             FROM s_grnplm_vd_hr_edp_srv_dq.tb_dq_rule tdr
             join (
             select * from (
             SELECT 
             a.rule_name
             ,row_number() over(partition by a.rule_name order by a.calc_dt desc) rn
             , a.stg_table
             , a.stg_column
             , a.load_dt, 
             case when (rul.rule_upper_value < cast(coalesce(a.max_date_diff,null,9999) as numeric)) then 0 else 1 end is_passed,
             (a.max_date_diff) actual_value, a.calc_dt
             ,row_number() over(partition by a.rule_name order by a.calc_dt desc)
             from
             s_grnplm_vd_hr_edp_srv_dq.tb_dq_max_date a
             join
             s_grnplm_vd_hr_edp_srv_dq.tb_dq_rule rul
             on a.rule_name = rul.rule_name
             ) as sect
             where rn=1
             order by 1) max_date 
             on tdr.rule_name = max_date.rule_name
             union
             select
             tdr.rule_name, rule_table_name, rule_field_name,
               rule_type_cd,  rule_upper_value control_value, actual_value, 
                is_passed, 
               load_dt
             FROM s_grnplm_vd_hr_edp_srv_dq.tb_dq_rule tdr
             join (
             select * from (
             SELECT 
             st.rule_name
             ,row_number() over(partition by st.rule_name order by st.calc_dt desc) rn
             , st.stg_table
             , st.stg_column
             , st.load_dt, 
             case when (rul.rule_upper_value < cast(st.psivalue as numeric)) then 0 else 1 end is_passed,
             cast(st.psivalue as numeric) actual_value, st.calc_dt
             ,row_number() over(partition by st.rule_name order by st.calc_dt desc)
             from
             s_grnplm_vd_hr_edp_srv_dq.tb_dq_stability st
             join
             s_grnplm_vd_hr_edp_srv_dq.tb_dq_rule rul
             on st.rule_name = rul.rule_name
             ) as sect
             where rn=1
             order by 1) stability
             on tdr.rule_name = stability.rule_name
             union
             select
             tdr.rule_name, rule_table_name, rule_field_name,
               rule_type_cd,  rule_upper_value control_value, actual_value, 
                is_passed, 
               load_dt
             FROM s_grnplm_vd_hr_edp_srv_dq.tb_dq_rule tdr
             join (
             select * from (
             SELECT 
             nr.rule_name
             ,row_number() over(partition by nr.rule_name order by nr.calc_dt desc) rn
             , nr.stg_table
             , nr.stg_column
             , nr.load_dt, 
             case when (rul.rule_upper_value > cast(nr.nonnull_ratio as numeric)) then 0 else 1 end is_passed
             , cast(nr.nonnull_ratio as numeric) actual_value, nr.calc_dt
             ,row_number() over(partition by nr.rule_name order by nr.calc_dt desc)
             from
             s_grnplm_vd_hr_edp_srv_dq.tb_dq_nonnull_ratio nr
             join
             s_grnplm_vd_hr_edp_srv_dq.tb_dq_rule rul
             on nr.rule_name = rul.rule_name
             ) as sect
             where rn=1
             order by 1) nonnull_ratio 
             on tdr.rule_name = nonnull_ratio.rule_name
                    --agg_comp
             union
             select
             tdr.rule_name, rule_table_name, rule_field_name,
               rule_type_cd,  rule_upper_value control_value, actual_value, 
                is_passed, 
               load_dt
             FROM s_grnplm_vd_hr_edp_srv_dq.tb_dq_rule tdr
             join (
             select * from (
             SELECT 
             ac.rule_name
             ,row_number() over(partition by ac.rule_name order by ac.calc_dt desc) rn
             , ac.stg_table
             , ac.stg_column
             , ac.load_dt
             ,case when (rul.rule_upper_value < cast(ac.agg_pct as numeric)) then 0 else 1 end is_passed
             , cast(ac.agg_pct as numeric) actual_value, ac.calc_dt
             ,row_number() over(partition by ac.rule_name order by ac.calc_dt desc)
             from
             s_grnplm_vd_hr_edp_srv_dq.tb_dq_aggregate_comp ac
             join
             s_grnplm_vd_hr_edp_srv_dq.tb_dq_rule rul
             on ac.rule_name = rul.rule_name
             ) as sect
             where rn=1
             order by 1) agg_pct
             on tdr.rule_name = agg_pct.rule_name
             union
             select
             tdr.rule_name, rule_table_name, rule_field_name,
               rule_type_cd,  rule_upper_value control_value, actual_value, 
                is_passed, 
               load_dt
             FROM s_grnplm_vd_hr_edp_srv_dq.tb_dq_rule tdr
             join (
             select * from (
             SELECT 
             us.rule_name
             ,row_number() over(partition by us.rule_name order by us.calc_dt desc) rn
             , null
             , null
             , us.load_dt
             ,case when (rul.rule_upper_value < cast(us.calc_value as numeric)) then 0 else 1 end is_passed
             , cast(us.calc_value as numeric) actual_value, us.calc_dt
             ,row_number() over(partition by us.rule_name order by us.calc_dt desc)
             from
             s_grnplm_vd_hr_edp_srv_dq.tb_dq_user_sql us
             join
             s_grnplm_vd_hr_edp_srv_dq.tb_dq_rule rul
             on us.rule_name = rul.rule_name
             ) as sect
             where rn=1
             order by 1) agg_pct
             on tdr.rule_name = agg_pct.rule_name
             order by 1, load_dt);
             update 
             s_grnplm_vd_hr_edp_srv_dq.tb_dq_check_status_hist
            set is_passed = 0 
             where actual_value is null;
            update s_grnplm_vd_hr_edp_srv_dq.tb_dq_check_status
             set is_passed =0
             where actual_value is null;
             UPDATE s_grnplm_vd_hr_edp_srv_dq.tb_dq_check_status_hist
             set effective_to = current_date where 
             rule_name in (select rule_name from s_grnplm_vd_hr_edp_srv_dq.tb_dq_check_status)
             and effective_to = '9999-12-31';
             INSERT INTO s_grnplm_vd_hr_edp_srv_dq.tb_dq_check_status_hist
             (rule_name, rule_table_name, rule_field_name, rule_type_cd, control_value, actual_value, is_passed, load_dt, effective_from, effective_to)
             SELECT rule_name, rule_table_name, rule_field_name, rule_type_cd, control_value, actual_value, is_passed, load_dt, current_date, '9999-12-31'
             FROM s_grnplm_vd_hr_edp_srv_dq.tb_dq_check_status;
end; 

$body$
EXECUTE ON ANY;
	