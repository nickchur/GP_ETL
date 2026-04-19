CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_update_contractor_movement_cdm_old() 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

	

	declare
		log_id integer;
		m_txt text;
		e_detail text;
		e_hint text;
		e_context text;
	begin
		log_id := s_grnplm_vd_hr_edp_srv_wf.pr_Log_start('GPUPDATE_contractor_movement_cdm (pr_update_contractor_movement_cdm)');
		
		begin
			if (
				(select 1 from s_grnplm_vd_hr_edp_stg.tb_oss_sep limit 1) is null
				or (select 1 from s_grnplm_vd_hr_edp_stg.tb_profile limit 1) is null
				or (select 1 from s_grnplm_vd_hr_edp_stg.tb_employee_movement_sm limit 1) is null
			) then
				m_txt = 'No data in one of the tables';
				perform s_grnplm_vd_hr_edp_srv_wf.pr_log_error(log_id,m_txt,'','','');
				return m_txt;
			end if;
			
			truncate s_grnplm_vd_hr_edp_stg.tb_contractor_movement_cdm;
			
			-- создаем таблицу, связывающую ШД-ТН-ОЕ на имеющиеся в ОШС даты
			create temp table oss_profile (
				load_date timestamp,
				report_date date,
				actual_date timestamp,
				pos_id int4,
				i_pernr int4,
				org_id int4
			)
			on commit drop
			distributed by (
				pos_id,
				i_pernr
			);
			
			-- игнорируем ТН без ШД (неустроенный ТН) и ШД без ТН (пустые ШД)
			insert into oss_profile (
				load_date,
				report_date,
				actual_date,
				pos_id,
				i_pernr,
				org_id
			)
			select
				oss.load_date,
				oss.report_date,
				oss.actual_date,
				profile.pos_id,
				profile.i_pernr,
				oss.org_id
			from
				(
					select
						load_date,
						report_date,
						actual_date,
						pos_id,
						org_id
					from
						s_grnplm_vd_hr_edp_stg.tb_oss_sep
					where
						lvl_01_code = 10283181
						and lvl_02_code = 10324575
				) as oss
				join s_grnplm_vd_hr_edp_stg.tb_profile as profile
					on profile.staff_category_id = 'ZP'
					and oss.report_date = profile.report_date
					and oss.pos_id = profile.pos_id
			;
			
			analyze oss_profile;
			
			-- создаем таблицу, которая по ТН добавляет его мероприятия
			create temp table movement_tmp (
				i_pernr int4,
				action_date date,
				action_type varchar(2)
			)
			on commit drop
			distributed by (
				i_pernr,
				action_date
			);
			
			-- игнорируем мероприятия без ТН (когда у профиля или ОШС глубина меньше)
			insert into movement_tmp (
				i_pernr,
				action_date,
				action_type
			)
			select
				main.i_pernr,
				case
					when main.action_type = 'S4' then main.action_date - 1
					else main.action_date
				end as action_date,
				main.action_type
			from
				s_grnplm_vd_hr_edp_stg.tb_employee_movement_sm as main
			where
				main.i_pernr in (select i_pernr from oss_profile)
			;
			
			analyze movement_tmp;
			
			-- после каждого S4 (увольнение с приемом) добавляем S3 (повторный прием),
			-- чтобы четные цифры означали увольнение, а нечетные - прием
			insert into movement_tmp (
				i_pernr,
				action_date,
				action_type
			)
			select
				main.i_pernr,
				main.action_date + 1,
				'S3' as action_type
			from
				movement_tmp as main
			where
				main.action_type = 'S4'
			;
			
			analyze movement_tmp;
			
			-- создаем таблицу, которая объединит мероприятия в интервалы действия договора
			-- action_date, action_type -> date_start, date_finish
			create temp table movement_cdm_tmp (
				i_pernr int4,
				date_start date,
				date_finish date
			)
			on commit drop
			distributed by (
				i_pernr
			);
			
			-- сначала конвертируем тип мероприятия в флаг, объединяющий 2 записи в границы интервала
			-- потом объединяем записи одного договора в один интервал, игнорируя интервалы без даты начала
			insert into movement_cdm_tmp (
				i_pernr,
				date_start,
				date_finish
			)
			select distinct
				main.i_pernr,
				min(main.action_date) over(
					partition by
						main.i_pernr,
						main.action_type
				) as date_start,
				case
					when 2 = (count(1) over (
						partition by
							main.i_pernr,
							main.action_type
					)) then
						max(main.action_date) over (
							partition by
								main.i_pernr,
								main.action_type
						)
					else '9999-12-31'::date
				end as date_finish
			from (
				select
					movement.i_pernr,
					movement.action_date,
					case
						when movement.action_type in ('S1', 'S3') then 
							1 + row_number() over (partition by i_pernr order by action_date)
						when movement.action_type in ('S2', 'S4') then 
							0 + row_number() over (partition by i_pernr order by action_date)
					end as action_type
				from 
					movement_tmp as movement
			) as main
			where
				main.action_type <> 1
			;
			
			analyze movement_cdm_tmp;
			
			-- объединяем мероприятия и информацию о сотрудниках по ТН
			insert into s_grnplm_vd_hr_edp_stg.tb_contractor_movement_cdm (
				load_date,
				report_date,
				actual_date,
				i_pernr,
				per_fio,
				date_start,
				date_finish,
				org_id,
				org_name,
				pos_id,
				pos_name,
				request_number,
				contract_number,
				specification_number,
				fblock_oss_tribe,
				fblock_oss_tribe_name,
				tribe_id,
				tribe_name,
				cluster_id,
				cluster_name,
				team_id,
				team_name,
				position_in_team_id,
				expertise_area
			)
			select
				per_info.load_date,
				per_info.report_date as report_date,
				per_info.actual_date,
				main.i_pernr as i_pernr,
				nullif(concat(pdata.surname, ' ' || pdata.firstname, ' ' || pdata.middlename), '') as per_fio,
				main.date_start as date_start,
				main.date_finish as date_finish,
				per_info.org_id as org_id,
				org_ed.org_mid as org_name,
				per_info.pos_id as pos_id,
				null as pos_name,
				null as request_number,
				null as contract_number,
				null as specification_number,
				fos.fblock_oss_tribe as fblock_oss_tribe,
				rfb.fblock_name as fblock_oss_tribe_name,
				fos.tribe_id as tribe_id,
				rt.tribe_name as tribe_name,
				fos.cluster_id as cluster_id,
				rc.cluster_name as cluster_name,
				fos.team_id as team_id,
				rteam.team_name as team_name,
				fos.pos_team_id as position_in_team_id,
				fos.expertise_area as expertise_area
			from
				movement_cdm_tmp as main
				join oss_profile as per_info
					on main.i_pernr = per_info.i_pernr
					and per_info.report_date between main.date_start and main.date_finish
				left join s_grnplm_vd_hr_edp_stg.tb_personal_data_sm as pdata
					on main.i_pernr = pdata.i_pernr
					and per_info.report_date between pdata.date_start and pdata.date_finish
				left join s_grnplm_vd_hr_edp_stg.tb_fos_sm as fos
					on per_info.report_date = fos.report_date
					and per_info.pos_id = fos.pos_id
				left join s_grnplm_vd_hr_edp_stg.tb_ref_fblock as rfb
					on fos.fblock_oss_tribe = rfb.fblock_oss
				left join s_grnplm_vd_hr_edp_stg.tb_ref_tribe_hist_cdm as rt
					on fos.tribe_id = rt.tribe_id
					and per_info.report_date between rt.date_start and rt.date_finish
				left join s_grnplm_vd_hr_edp_stg.tb_ref_cluster_hist_cdm as rc
					on fos.cluster_id = rc.cluster_id
					and per_info.report_date between rc.date_start and rc.date_finish
				left join s_grnplm_vd_hr_edp_stg.tb_ref_team_hist_cdm as rteam
					on fos.team_id = rteam.team_id
					and per_info.report_date between rteam.date_start and rteam.date_finish
				left join s_grnplm_vd_hr_edp_stg.tb_ref_org_hist_cdm as org_ed
					on per_info.org_id = org_ed.org_id
					and per_info.report_date between org_ed.date_start and org_ed.date_finish
			;
			
			analyze s_grnplm_vd_hr_edp_stg.tb_contractor_movement_cdm;

			perform s_grnplm_vd_hr_edp_srv_wf.pr_log_skew('s_grnplm_vd_hr_edp_stg.tb_contractor_movement_cdm');

			perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_end(log_id,'s_grnplm_vd_hr_edp_stg.tb_contractor_movement_cdm','report_date','load_date','actual_date');
			return 'OK';

		exception when OTHERS then
			get stacked diagnostics m_txt = MESSAGE_TEXT;
			get stacked diagnostics e_detail = PG_EXCEPTION_DETAIL;
			get stacked diagnostics e_hint = PG_EXCEPTION_HINT;
			get stacked diagnostics e_context = PG_EXCEPTION_CONTEXT;
			
			perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_error(log_id,m_txt,e_detail,e_hint,e_context) ;
			return m_txt;

		end;
	end;



$body$
EXECUTE ON ANY;
	