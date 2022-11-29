use sales;
go

/*

drop view current_snapshot_analysis_final;

select * from current_snapshot_analysis_final;

*/

-- se ajusta rampa a siete meses.
-- se incluye info de demo
-- se incluyen fechas y parámetros de oportunidad.

-- v.31. agregar first_closed_deal info para la compañía hija
-- v.33. ajustar pipeline SMB
-- v.34. Ajustar account_executive
-- v.35. Se cambia el tope de weeks_since_entry a 12 desde 8
-- v.36. Se incluye tiempos de etapa en lost
-- v.37. incluye arr neto
-- v.38. Incluye Razón Social, entre otros
-- v.39. Associated Vids

--create view current_snapshot_analysis_final as
with company_adj as (
	select a.*, case when a.industriaespecifica_label is null then b.industria_equivalent else a.industriaespecifica_label end industriaespecifica_label_adj
	from [dbo].[companies_last_snapshot] a
	left join [dbo].[dim_industria_equivalent] b on (a.industriaespecifica = b.industria_old)
	--where companyId = 6023864702
), sls_data as (
	select --sls.*,
	sls.dealid,
	sls.dealname,
	company."name" company_name,
	coalesce(parent_company."name",company."name",'unidentified') parent_company_name,
	company.companyid company_id,
	parent_company.companyid parent_company_id,
	sls.associatedVids,
	coalesce(country_sd.standard_country,cs.standard_country,'unidentified') deal_country,
	coalesce(company.industriaespecifica_label_adj,parent_company.industriaespecifica_label_adj,sls.industriaespecifica_label,'Unidentified') industry,
	sls.hubspot_owner_id,
	coalesce(astage.final_stage ,stages.stage) stage,
	stages.pipeline as pipeline_name,
	sls.createdate,
	CAST(sls.createdate as date) create_date_val,
	coalesce(a.closedate_modificado,sls.closedate) closedate,
	case when sls.createdate > coalesce(a.closedate_modificado,sls.closedate) + 180 and stages.stage = 'In' then sls.createdate else coalesce(a.closedate_modificado,sls.closedate) end finalclosedate,
	case when sls.hs_createdate > coalesce(a.closedate_modificado,sls.closedate) + 30 and stages.stage = 'In' then 1 else 0 end closedateadjustmentflag,
	case when stages.pipeline = 'Partner' then coalesce (sls.hs_date_entered_2329182,hs_date_entered_2915613)
				   when stages.pipeline = 'Sales' then hs_date_entered_2329182
				   else hs_date_entered_2915613
				   end hs_enter_in_demo,
	case when (case when stages.pipeline = 'Partner' then coalesce (sls.hs_date_entered_contractsent,sls.hs_date_entered_8f18c08a_f9a2_471a_b57f_01a37fca6a38_1999360086)
				   when stages.pipeline = 'Sales' then sls.hs_date_entered_contractsent
				   else sls.hs_date_entered_8f18c08a_f9a2_471a_b57f_01a37fca6a38_1999360086
				   end) between coalesce(a.closedate_modificado,sls.closedate) + 1 and coalesce(a.closedate_modificado,sls.closedate) + 365
		 then coalesce(a.closedate_modificado,sls.closedate)
		 else (case when stages.pipeline = 'Partner' then coalesce (sls.hs_date_entered_contractsent,sls.hs_date_entered_8f18c08a_f9a2_471a_b57f_01a37fca6a38_1999360086)
				   when stages.pipeline = 'Sales' then sls.hs_date_entered_contractsent
				   else sls.hs_date_entered_8f18c08a_f9a2_471a_b57f_01a37fca6a38_1999360086
				   end)
		 end as hs_enter_in_closedwon,
	--case when stages.pipeline = 'Sales' then sls.hs_date_entered_2329182
	--	 when stages.pipeline = 'KAM' then sls.hs_date_entered_2915613
	--	 when stages.pipeline = 'Sales SMB' then sls.hs_date_entered_2235010 
	--	 else NULL end enteredDate_FreeTrial,
	--case when stages.pipeline = 'Sales' then sls.hs_date_exited_2329182
	--	 when stages.pipeline = 'KAM' then sls.hs_date_exited_2915613
	--	 when stages.pipeline = 'Sales SMB' then sls.hs_date_exited_2235010
	--	 else NULL end ExitedDate_FreeTrial,
	--case when stages.pipeline = 'Sales' then sls.hs_time_in_2329182
	--	 when stages.pipeline = 'KAM' then sls.hs_time_in_2915613
	--	 when stages.pipeline = 'Sales SMB' then sls.hs_time_in_2235010
	--	 else NULL end StageTime_FreeTrial,

	sls.createdate enteredDate_SQL,
	coalesce(case when stages.pipeline = 'Sales' then sls.hs_date_exited_qualifiedtobuy
		 when stages.pipeline = 'KAM' then sls.hs_date_exited_167293
		 when stages.pipeline = 'Sales SMB' then sls.hs_date_exited_2235011
		 else NULL end,case when stages.pipeline = 'Sales' then sls.hs_date_exited_2329182
		 when stages.pipeline = 'Sales SMB' then sls.hs_date_exited_2235010
		 else NULL end) ExitedDate_SQL,
	cast(1000 as bigint)*datediff(s, sls.createdate,
	coalesce(case when stages.pipeline = 'Sales' then sls.hs_date_exited_qualifiedtobuy
		 when stages.pipeline = 'KAM' then sls.hs_date_exited_167293
		 when stages.pipeline = 'Sales SMB' then sls.hs_date_exited_2235011
		 else NULL end,case when stages.pipeline = 'Sales' then sls.hs_date_exited_2329182
		 when stages.pipeline = 'Sales SMB' then sls.hs_date_exited_2235010
		 else NULL end)) StageTime_SQL,
	
	--case when stages.pipeline = 'Sales' then sls.hs_date_entered_qualifiedtobuy
	--	 when stages.pipeline = 'KAM' then sls.hs_date_entered_167293
	--	 when stages.pipeline = 'Sales SMB' then sls.hs_date_entered_2235011
	--	 when stages.pipeline = 'Partner' then sls.hs_date_entered_610404 
	--	 else NULL end enteredDate_Pipeline,
	--case when stages.pipeline = 'Sales' then sls.hs_date_exited_qualifiedtobuy
	--	 when stages.pipeline = 'KAM' then sls.hs_date_exited_167293
	--	 when stages.pipeline = 'Sales SMB' then sls.hs_date_exited_2235011
	--	 when stages.pipeline = 'Partner' then sls.hs_date_exited_610404
	--	 else NULL end ExitedDate_Pipeline,
	--case when stages.pipeline = 'Sales' then sls.hs_time_in_qualifiedtobuy
	--	 when stages.pipeline = 'KAM' then sls.hs_time_in_167293
	--	 when stages.pipeline = 'Sales SMB' then sls.hs_time_in_2235011
	--	 when stages.pipeline = 'Partner' then sls.hs_time_in_610404
	--	 else NULL end StageTime_Pipeline,

	case when stages.pipeline = 'Sales' and sls.createdate <= cast('2021-11-15' as date) then sls.hs_date_entered_qualifiedtobuy
		 when stages.pipeline = 'Sales' and sls.createdate between cast('2021-11-16' as date) and cast('2021-12-15' as date) then sls.hs_date_entered_qualifiedtobuy
		 when stages.Pipeline = 'Sales' then sls.hs_date_entered_9860635
		 when stages.pipeline = 'KAM' then sls.hs_date_entered_167293
		 when stages.pipeline = 'Sales SMB' and sls.createdate <= cast('2021-11-15' as date) then sls.hs_date_entered_2235011
		 when stages.pipeline = 'Sales SMB' then hs_date_entered_13750164
		 when stages.pipeline = 'Partner' then sls.hs_date_entered_610404 
		 else NULL end enteredDate_Opportunity,
	case when stages.pipeline = 'Sales' and sls.createdate <= cast('2021-11-15' as date) then sls.hs_date_exited_qualifiedtobuy
		 when stages.Pipeline = 'Sales' then sls.hs_date_exited_9860635
		 when stages.pipeline = 'KAM' then sls.hs_date_exited_167293
		 when stages.pipeline = 'Sales SMB' and sls.createdate <= cast('2021-11-15' as date) then sls.hs_date_exited_2235011
		 when stages.pipeline = 'Sales SMB' then hs_date_exited_13750164
		 when stages.pipeline = 'Partner' then sls.hs_date_exited_610404
		 else NULL end ExitedDate_Opportunity,
	case when stages.pipeline = 'Sales' and sls.createdate <= cast('2021-11-15' as date) then sls.hs_time_in_qualifiedtobuy
		 when stages.Pipeline = 'Sales' then sls.hs_time_in_9860635
		 when stages.pipeline = 'KAM' then sls.hs_time_in_167293
		 when stages.pipeline = 'Sales SMB' and sls.createdate <= cast('2021-11-15' as date) then sls.hs_time_in_2235011
		 when stages.pipeline = 'Sales SMB' then hs_time_in_13750164
		 when stages.pipeline = 'Partner' then sls.hs_time_in_610404
		 else NULL end StageTime_Opportunity,

	case when stages.pipeline = 'Sales' then sls.hs_date_entered_presentationscheduled
		 when stages.pipeline = 'KAM' then sls.hs_date_entered_183f0bca_0aa5_4a62_b94b_2f9b0d41b460_477251997
		 when stages.pipeline = 'Sales SMB' then sls.hs_date_entered_2235012
		 when stages.pipeline = 'Partner' then sls.hs_date_entered_610405
		 else NULL end enteredDate_Upside,
	case when stages.pipeline = 'Sales' then sls.hs_date_exited_presentationscheduled
		 when stages.pipeline = 'KAM' then sls.hs_date_exited_183f0bca_0aa5_4a62_b94b_2f9b0d41b460_477251997
		 when stages.pipeline = 'Sales SMB' then sls.hs_date_exited_2235012
		 when stages.pipeline = 'Partner' then sls.hs_date_exited_610405
		 else NULL end ExitedDate_Upside,
	case when stages.pipeline = 'Sales' then sls.hs_time_in_presentationscheduled
		 when stages.pipeline = 'KAM' then sls.hs_time_in_183f0bca_0aa5_4a62_b94b_2f9b0d41b460_477251997
		 when stages.pipeline = 'Sales SMB' then sls.hs_time_in_2235012
		 when stages.pipeline = 'Partner' then sls.hs_time_in_610405
		 else NULL end StageTime_Upside,

	case when stages.pipeline = 'Sales' then sls.hs_date_entered_decisionmakerboughtin
		 when stages.pipeline = 'KAM' then sls.hs_date_entered_30ca13bf_5b5f_4b6d_911d_7b21ad88f01a_768078784
		 when stages.pipeline = 'Sales SMB' then sls.hs_date_entered_2235013
		 when stages.pipeline = 'Partner' then sls.hs_date_entered_610406
		 else NULL end enteredDate_Forecast,
	case when stages.pipeline = 'Sales' then sls.hs_date_exited_decisionmakerboughtin
		 when stages.pipeline = 'KAM' then sls.hs_date_exited_30ca13bf_5b5f_4b6d_911d_7b21ad88f01a_768078784
		 when stages.pipeline = 'Sales SMB' then sls.hs_date_exited_2235013
		 when stages.pipeline = 'Partner' then sls.hs_date_exited_610406
		 else NULL end ExitedDate_Forecast,
	case when stages.pipeline = 'Sales' then sls.hs_time_in_decisionmakerboughtin
		 when stages.pipeline = 'KAM' then sls.hs_time_in_30ca13bf_5b5f_4b6d_911d_7b21ad88f01a_768078784
		 when stages.pipeline = 'Sales SMB' then sls.hs_time_in_2235013
		 when stages.pipeline = 'Partner' then sls.hs_time_in_610406
		 else NULL end StageTime_Forecast,

	case when stages.pipeline = 'Sales' then coalesce(sls.hs_date_entered_closedwon, sls.hs_date_entered_contractsent)
		 when stages.pipeline = 'KAM' then sls.hs_date_entered_8f18c08a_f9a2_471a_b57f_01a37fca6a38_1999360086
		 when stages.pipeline = 'Sales SMB' then sls.hs_date_entered_2235014
		 when stages.pipeline = 'Partner' then sls.hs_date_entered_610407
		 else NULL end enteredDate_In,
	case when stages.pipeline = 'Sales' then coalesce(sls.hs_date_exited_closedwon, sls.hs_date_exited_contractsent)
		 when stages.pipeline = 'KAM' then sls.hs_date_exited_8f18c08a_f9a2_471a_b57f_01a37fca6a38_1999360086
		 when stages.pipeline = 'Sales SMB' then sls.hs_date_exited_2235014
		 when stages.pipeline = 'Partner' then sls.hs_date_exited_610407
		 else NULL end ExitedDate_In,
	case when stages.pipeline = 'Sales' then coalesce(sls.hs_time_in_closedwon, sls.hs_time_in_contractsent)
		 when stages.pipeline = 'KAM' then sls.hs_time_in_8f18c08a_f9a2_471a_b57f_01a37fca6a38_1999360086
		 when stages.pipeline = 'Sales SMB' then sls.hs_time_in_2235014
		 when stages.pipeline = 'Partner' then sls.hs_time_in_610407
		 else NULL end StageTime_In,

	case when stages.pipeline = 'Sales' then sls.hs_date_entered_closedlost
		 when stages.pipeline = 'KAM' then sls.hs_date_entered_f894ab48_6e0a_4652_abf5_07987478b71a_1819638532
		 when stages.pipeline = 'Sales SMB' then sls.hs_date_entered_2235015
		 when stages.pipeline = 'Partner' then sls.hs_date_entered_610415
		 else NULL end enteredDate_Lost,
	case when stages.pipeline = 'Sales' then sls.hs_time_in_closedlost
		 when stages.pipeline = 'KAM' then sls.hs_time_in_f894ab48_6e0a_4652_abf5_07987478b71a_1819638532
		 when stages.pipeline = 'Sales SMB' then sls.hs_time_in_2235015
		 when stages.pipeline = 'Partner' then sls.hs_time_in_610415
		 else NULL end StageTime_Lost,

	case when coalesce(astage.final_stage ,stages.stage) = 'In' then 1 else 0 end as closed_deal_flag,
	max(case when coalesce(astage.final_stage ,stages.stage) = 'In' then 1 else 0 end) over (partition by parent_company.companyid) closed_deal_company_flag,
	case when cast(getdate()+30000 as date) = min(case when coalesce(astage.final_stage ,stages.stage) = 'In' then coalesce(a.closedate_modificado,sls.closedate) else cast(getdate()+30000 as date) end) over (partition by parent_company.companyid)
			  or coalesce(parent_company."name",company."name",'unidentified') = 'unidentified'
		 then null
		 else min(case when coalesce(astage.final_stage ,stages.stage) = 'In' then coalesce(a.closedate_modificado,sls.closedate) else cast(getdate()+30000 as date) end) over (partition by parent_company.companyid)
		 end earliest_deal_closedate,
	case when cast(getdate()+30000 as date) = min(case when coalesce(astage.final_stage ,stages.stage) = 'In' then coalesce(a.closedate_modificado,sls.closedate) else cast(getdate()+30000 as date) end) over (partition by company.companyid)
			  or coalesce(company."name",'unidentified') = 'unidentified'
		 then null
		 else min(case when coalesce(astage.final_stage ,stages.stage) = 'In' then coalesce(a.closedate_modificado,sls.closedate) else cast(getdate()+30000 as date) end) over (partition by company.companyid)
		 end earliest_deal_closedate_child_company,
	case when coalesce(parent_company."name",company."name",'unidentified') = 'unidentified'
		 then null
		 else first_value(case when coalesce(astage.final_stage ,stages.stage) = 'In' then sls.dealid else null end)
					over(partition by parent_company.companyid
						 order by case when coalesce(astage.final_stage ,stages.stage) = 'In' then coalesce(a.closedate_modificado,sls.closedate) else cast(getdate()+30000 as date) end asc)
		 end first_closed_deal_id,
	case when coalesce(parent_company."name",company."name",'unidentified') = 'unidentified'
		 then null
		 else first_value(case when coalesce(astage.final_stage ,stages.stage) = 'In' then sls.createdate else null end)
					over(partition by parent_company.companyid
						 order by case when coalesce(astage.final_stage ,stages.stage) = 'In' then coalesce(a.closedate_modificado,sls.closedate) else cast(getdate()+30000 as date) end asc)
		 end first_closed_deal_createdate,
	case when coalesce(company."name",'unidentified') = 'unidentified'
		 then null
		 else first_value(case when coalesce(astage.final_stage ,stages.stage) = 'In' then sls.dealid else null end)
					over(partition by company.companyid
						 order by case when coalesce(astage.final_stage ,stages.stage) = 'In' then coalesce(a.closedate_modificado,sls.closedate) else cast(getdate()+30000 as date) end asc)
		 end first_closed_deal_id_child_company,
	case when coalesce(company."name",'unidentified') = 'unidentified'
		 then null
		 else first_value(case when coalesce(astage.final_stage ,stages.stage) = 'In' then sls.createdate else null end)
					over(partition by company.companyid
						 order by case when coalesce(astage.final_stage ,stages.stage) = 'In' then coalesce(a.closedate_modificado,sls.closedate) else cast(getdate()+30000 as date) end asc)
		 end first_closed_deal_createdate_child_company,
	company.recent_deal_close_date,
	company.first_deal_created_date,
	sls.amount,
	amount_in_home_currency,
	coalesce(company.numberofemployees,parent_company.numberofemployees,0) company_employee_q,
	sls.n_mero_de_empleados deal_company_employee_q,
	sls.licencias_vendidas,
	sls.source,
	sls.n_mero_de_pagos,
	sls.plazo_de_pago,
	sls.tipo_negocio_kam,
	coalesce(kam.firstname + ' ' + kam.lastname, 'unidentified') kam,
	sdr.nombre sdr,
	acc_exe.nombre account_executive,
	sls.account_executive_team,
	case when sls.n_mero_de_pagos is not null and sls.n_mero_de_pagos > 0 then 1 else 0 end flag_num_pagos,
	case when sls.plazo_de_pago is not null and sls.plazo_de_pago != '' then 1 else 0 end flag_plazo_de_pago,
	sls.fecha_inicio_prueba_30_dias,
	sls.fecha_fin_prueba_30_dias,
	sls.fecha_solicitud_prueba_30_dias,
	sls.demo_30_days_label,
	sls.semanas_en_demo_label,
	sls.fecha_cambio_a_opportunity,
	sdr.ingreso sdr_ingreso,
	sls.arr_neto,
	sls.deal_currency_code,
	sls.razon_social,
	sls.nit
	from [dbo].[deals_last_snapshot] as sls
	left join [dbo].[dim_stages] as stages on (sls.dealstage = stages.key_string)
	left join company_adj company on (substring(sls.associatedcompanyids,1,case when charindex(',',sls.associatedcompanyids) = 0 then 10000 else charindex(',',sls.associatedcompanyids) - 1 end) = cast(company.companyid as nvarchar))
	left join [dbo].[ajustes_company] ajuste_parent_company on (sls.dealid = ajuste_parent_company.deal_id)
	left join company_adj parent_company on (coalesce(ajuste_parent_company.parent_company_id,company.hs_parent_company_id,company.companyid,'0000000000') = parent_company.companyid)
	left join [dbo].[owners_last_snapshot] kam on (sls.kam_asignad_ = kam.ownerid)
	left join [dbo].planta_vw sdr on (sls.sdr = sdr.owner_id and cast(sls.createdate as date) between sdr.valid_from and sdr.valid_to)
	left join [dbo].planta_vw acc_exe on (sls.account_executive = acc_exe.owner_id and cast(sls.createdate as date) between acc_exe.valid_from and acc_exe.valid_to)
	left join [dbo].[dim_country_standardization] country_sd on (company.country = country_sd.pais_hs)
	left join [dbo].[dim_country_standardization] cs on (sls.pais = cs.pais_hs)
	left join [dbo].[ajustes_preliminar_base] a on (sls.dealid = a.dealid)
	left join [dbo].[ajustes_stage_preliminar] astage on (sls.dealid = astage.deal_id)
--where
--sls.dealid = 5258360340 -- and
--parent_company.companyid = 3919190184
) --select * from sls_data where dealid = 469047215
, property_history_full as (
	select "deal.dealid" as dealid, coalesce(stages.stage, 'unidentified') as stageid, coalesce(stages.stage, 'unidentified') + '2' as stageid_2, max("timestamp") as maxchangedate
	from [dbo].[deals_propertyhistory]
	left join [dbo].[dim_stages] stages on ([dbo].[deals_propertyhistory].[value] = stages.key_string)
	left join [dbo].[deal_exclusions] e on ("deal.dealId" = e.dealid and stages.Stage = e.dealstage)
	where name = 'dealstage'
	and e.dealid is null
	group by "deal.dealid", coalesce(stages.stage, 'unidentified')
) -- select * from property_history_full
, property_history as (
	select *, row_number() over(partition by dealid order by maxchangedate desc) as propertychangeid
	from property_history_full
) -- select * from property_history
, propertystagehistory as (
	select dealid, max([1]) as laststage, max([2]) as previousstage--, max(stages_ls.stage) as ls, max(stages_ps.stage) as ps
	from property_history
	pivot (min(stageid) for propertychangeid in ([1],[2])) as pv
	group by dealid
) --select * from propertystagehistory order by 1
, final_property_deal_stage_count as (
	select dealid,
	sum([SQL]) [SQL], sum([Opportunity]) [Opportunity], sum([Demo]) [Demo], sum([Pipeline]) [Pipeline], sum([Upside]) [Upside],sum([Forecast]) [Forecast],sum([In]) [In],sum([Lost]) [Lost],
	max([SQL2]) [SQL_In_Date], max([Opportunity2]) [Opportunity_In_Date], max([Demo2]) [Demo_In_Date], max([Pipeline2]) [Pipeline_In_Date], max([Upside2]) [Upside_In_Date], max([Forecast2]) [Forecast_In_Date], max([In2]) [In_In_Date], max([Lost2]) [Lost_In_Date]
	from property_history_full a
	pivot (count(stageid) for stageid in ([SQL],[Opportunity],[Demo],[Pipeline],[Upside],[Forecast],[In],[Lost])) as pv
	pivot (max(maxchangedate) for stageid_2 in ([SQL2],[Opportunity2],[Demo2],[Pipeline2],[Upside2],[Forecast2],[In2],[Lost2])) as pv_2
	group by dealid
) --select * from final_property_deal_stage_count
, opportunity_history_recognition as (
	select dealid, max(maxchangedate) max_recognized_exited_starting_stage
	from property_history_full
	where stageid not in ('Demo','Pipeline','Opportunity')
	group by dealid
) --select * from opportunity_history_recognition where dealid = 3373895634
, final_data as (
	select sls_data.*,
	case when sls_data.amount_in_home_currency = 0
			  then 'Other'
		 when sls_data.tipo_negocio_kam in ('Venta inicial','Venta cruzada')
			  -- validar regla
			  or sls_data.dealid = sls_data.first_closed_deal_id
			  or ((sls_data.stage not in ('Lost','In')) and (sls_data.createdate <= sls_data.earliest_deal_closedate))
			  or sls_data.earliest_deal_closedate is null
			  then 'New ARR'
		 when sls_data.tipo_negocio_kam in ('Expansión','Upselling otro país')
			  then 'Expansion ARR'
		 when sls_data.tipo_negocio_kam in ('Renovación')
			  then 'Renovacion'
		 --when (sls_data.createdate > sls_data.earliest_deal_closedate) and sls_data.tipo_negocio_kam = 'renovación'
			  --then 'renovacion'
		 else 'Expansion arr' end arr_type,
	stageh.previousstage,
	planta.nombre as bd,
	planta.area as team,
	planta.pais as pais_ubits,
	planta.subteam,
	planta.subteam_active,
	close_dates.week_start close_week_start,
	close_dates.yyyy close_year,
	close_dates.mm close_month,
	close_dates.ww close_week,
	create_dates.week_start create_week_start,
	create_dates.yyyy create_year,
	create_dates.mm create_month,
	create_dates.ww create_week,
	-- finish referencing hubspot_user_id
	planta.ingreso,
	datediff(d,planta.ingreso,create_dates.dte) + 1 days_since_entry,
	datediff(ww,dateadd(dd,-1,planta.ingreso),dateadd(dd,-1,create_dates.dte)) + 1 actual_weeks_since_entry,
	datediff(mm,planta.ingreso,create_dates.dte) + case when datepart(d,planta.ingreso) <= 15 then 1 else 0 end actual_months_since_entry,
	case when datediff(ww,dateadd(dd,-1,planta.ingreso),dateadd(dd,-1,create_dates.dte)) + 1 <= 12
		 then datediff(ww,dateadd(dd,-1,planta.ingreso),dateadd(dd,-1,create_dates.dte)) + 1
		 else 13 end weeks_since_entry,
	case when datediff(mm,planta.ingreso,create_dates.dte) + case when datepart(d,planta.ingreso) <= 15 then 1 else 0 end <= 7
		 then datediff(mm,planta.ingreso,create_dates.dte) + case when datepart(d,planta.ingreso) <= 15 then 1 else 0 end
		 else 7 end months_since_entry,
	coalesce(creation_cohort.Name,'Unidentified') creation_cohort,
	datediff(d,planta.ingreso,close_dates.dte) + 1 days_since_entry_close,
	case when datediff(ww,dateadd(dd,-1,planta.ingreso),dateadd(dd,-1,close_dates.dte)) + 1 <= 12
		 then datediff(ww,dateadd(dd,-1,planta.ingreso),dateadd(dd,-1,close_dates.dte)) + 1
		 else 13 end weeks_since_entry_close,
	case when datediff(mm,planta.ingreso,close_dates.dte) + case when datepart(d,planta.ingreso) <= 15 then 1 else 0 end <= 7
		 then datediff(mm,planta.ingreso,close_dates.dte) + case when datepart(d,planta.ingreso) <= 15 then 1 else 0 end
		 else 7 end months_since_entry_close,
	coalesce(closing_cohort.Name,'Unidentified') close_cohort,
	substring(convert(varchar,sls_data.hs_enter_in_closedwon,102),1,7) as inmonth,
	substring(convert(varchar,sls_data.closedate,102),1,7) as closemonth,
	datediff("d",owners.createdat,sls_data.hs_enter_in_closedwon) as days_in_ru_to_close,
	1 deal_count,
	case when Stage = 'Demo' or s_count.Demo /*+ s_count.Pipeline + s_count.Upside + s_count.Forecast + s_count.[In]*/ > 0
			 then 1 else 0 end Demo_count,
	case when 1 = 1--(Stage = 'SQL')
			 --or (sls_data.Stage not in ('SQL') and s_count.Demo + s_count.[SQL] + s_count.Pipeline + s_count.Upside + s_count.Forecast + s_count.[In] > 0)
			 --or (sls_data.Stage = 'Lost' and s_count.Demo + s_count.[SQL] + s_count.Pipeline + s_count.Upside + s_count.Forecast + s_count.[In] = 0)
			 then 1 else 0 end SQL_count,
	case when Stage = 'Opportunity'
			 or (sls_data.Stage not in ('Demo','Pipeline','SQL') and s_count.Opportunity + s_count.Upside + s_count.Forecast + s_count.[In] > 0)
			 or (sls_data.Stage not in ('SQL') and createdate <= cast('2021-11-15' as date) and coalesce(ExitedDate_SQL,opp_recog.max_recognized_exited_starting_stage) <= cast('2021-12-15' as date))
		 then 1 else 0 end Opportunity_count,
	case when Stage = 'Upside'
			 or (sls_data.Stage not in ('Demo','Pipeline','SQL','Opportunity') and s_count.Upside + s_count.Forecast + s_count.[In] > 0)
		 then 1 else 0 end Upside_count,
	case when Stage = 'Forecast'
			 or (sls_data.Stage not in ('Demo','Pipeline','SQL','Opportunity','Upside') and s_count.Forecast + s_count.[In] > 0)
			  --or (Stage = 'Lost' and sls_data.previousStage in ('In','Forecast'))
			 then 1 else 0 end Forecast_count,
	case when Stage = 'In' then 1 else 0 end In_count,
	case when Stage = 'Lost' then 1 else 0 end Lost_count
	--, s_count.Opportunity, s_count.Opportunity_In_Date
	from sls_data
	left join [dbo].[owners_last_snapshot] as owners on (sls_data.hubspot_owner_id = owners.ownerid)
	left join propertystagehistory as stageh on (sls_data.dealid = stageh.dealid)
	left outer join [dbo].[dim_dates] create_dates on (cast(sls_data.createdate as date) = create_dates.dte)
	left outer join [dbo].[dim_dates] close_dates on (cast(sls_data.finalclosedate as date) = close_dates.dte)
	left join [dbo].[planta_vw] planta on (sls_data.hubspot_owner_id = planta.owner_id and close_dates.dte between planta.valid_from and planta.valid_to)
	left outer join [dbo].[dim_dates] planta_dates on (planta.ingreso = planta_dates.dte)
	left join [dbo].[dim_cohort_comercial] as creation_cohort on (planta.ingreso between creation_cohort.startdate and creation_cohort.enddate)
	left join [dbo].[dim_cohort_comercial] as closing_cohort on (planta.ingreso between closing_cohort.startdate and closing_cohort.enddate)
	left outer join final_property_deal_stage_count s_count on (sls_data.dealId = s_count.dealid)
	left join opportunity_history_recognition opp_recog on (sls_data.dealId = opp_recog.dealid)
	--where sls_data.dealid = 8546370142
) --select createdate a1, Opportunity_In_Date a2, DATEDIFF(d,createdate,Opportunity_In_Date) diff, * from final_data where (createdate > cast('2021-12-15' as date) and pipeline_name in ('Sales', 'Sales SMB') and Opportunity > 0) --dealid = 5384218491 --group by dealid having count(*) > 1
--select * from final_data where dealid = 469047215
, faltantes as (
select
a.deal_id, a.parent_company_id, b.[Change_SubKey],
case when b.Change_SubKey = 1 then null else a.ajuste_date end fecha_cambio,
case when b.Change_SubKey = 1 then null else b.[Change_SubName] end arr_type,
case when b.Change_SubKey = 1 then null else a.arr end arr,
case when b.Change_SubKey = 1 then null else a.licencias end licencias,
case when b.Change_SubKey = 1 then null else a.bd end bd
from [dbo].[ajustes_negocios_faltantes] a
left join [dbo].[dim_deals_modificaction] b on (b.change_key = 3)
) --select  * from final_data where dealid = 5233042798
select --* from final_data
f.dealid,
f.dealname,
f.company_name,
f.company_id,
f.associatedVids,
f.parent_company_name,
f.parent_company_id, 
coalesce(a.deal_country, f.deal_country) deal_country,
case when coalesce(i.industry,a.industria, f.industry) in ('FMCG','Services Companies','Agroindustry','Government','Financial Services & Insurance','Retail','Industry / Manufacture / Chemicals / Automotive','Oil & Gas / Energy  / Mining / Environment','Technology','Pharma & Healthcare','Transport & Logistics','Property & Construction')
	 then coalesce(i.industry,a.industria, f.industry)
	 else 'Unidentified' end
	 industry,
f.hubspot_owner_id,
f.stage stage,-- ajustar deals con cambio de stage
f.SQL_count [SQL],
f.Opportunity_count Opportunity,
f.Upside_count Upside,
f.Forecast_count Forecast,
f.In_count [In],
f.Lost_count Lost,
f.pipeline_name,
close_dates_final.yyyy close_year,
close_dates_final.mm close_month,
close_dates_final.ww close_week,
close_dates_final.week_start close_week_start,
coalesce(f.fecha_cambio,a.closedate_modificado,f.closedate) closedate,
coalesce(f.fecha_cambio,a.closedate_modificado,f.finalclosedate) final_close_date,
coalesce(closing_cohort_final.Name,'Unidentified') close_cohort,
f.create_year,
f.create_month,
f.create_week,
f.create_week_start,
f.create_date_val createdate,
f.creation_cohort,
f.closed_deal_flag,
f.closed_deal_company_flag,
coalesce(f.arr_faltante,a.arr,f.amount_in_home_currency)*coalesce(f.part_compartidos,1) amount_in_home_currency,
f.arr_neto,
f.deal_count,
case when row_number() over(partition by f.dealid order by coalesce(f.arr_faltante,a.arr,f.amount_in_home_currency)*coalesce(f.part_compartidos,1) desc, coalesce(f.bd_faltante, f.bd_compartido, f.account_executive, a.bd, f.bd)) = 1 then 1 else 0 end deal_unique_count,
coalesce(a.empleados, case when coalesce(a.empleados,case when patindex('%[^0-9]%',f.deal_company_employee_q) > 0 then null else cast(f.deal_company_employee_q as bigint) end) > f.company_employee_q
						   then coalesce(a.empleados,case when patindex('%[^0-9]%',f.deal_company_employee_q) > 0 then null else cast(f.deal_company_employee_q as bigint) end)
						   else f.company_employee_q end) company_employee_q,
coalesce(a.empleados,case when patindex('%[^0-9]%',f.deal_company_employee_q) > 0 then null else cast(f.deal_company_employee_q as bigint) end) deal_company_employee_q,
round(coalesce(f.licencias_faltante,a.licencias,f.licencias_vendidas)*coalesce(f.part_compartidos,1),0) licencias_vendidas,
case when coalesce(a.empleados,f.company_employee_q) is null or coalesce(a.empleados,f.company_employee_q) = 0 then 'Undefined'
	 when coalesce(a.empleados,f.company_employee_q) <= 10 then 'Micro'
	 when coalesce(a.empleados,f.company_employee_q) <= 50 then 'Small'
	 when coalesce(a.empleados,f.company_employee_q) <= 250 then 'Medium'
	 when coalesce(a.empleados,f.company_employee_q) <= 1000 then 'Large'
	 else 'Extra large' end employee_count_based_size,
case when coalesce(a.empleados, case when coalesce(a.empleados,case when patindex('%[^0-9]%',f.deal_company_employee_q) > 0 then null else cast(f.deal_company_employee_q as bigint) end) > f.company_employee_q
						   then coalesce(a.empleados,case when patindex('%[^0-9]%',f.deal_company_employee_q) > 0 then null else cast(f.deal_company_employee_q as bigint) end)
						   else f.company_employee_q end) is null
	   or coalesce(a.empleados, case when coalesce(a.empleados,case when patindex('%[^0-9]%',f.deal_company_employee_q) > 0 then null else cast(f.deal_company_employee_q as bigint) end) > f.company_employee_q
						   then coalesce(a.empleados,case when patindex('%[^0-9]%',f.deal_company_employee_q) > 0 then null else cast(f.deal_company_employee_q as bigint) end)
						   else f.company_employee_q end) = 0 then 'Undefined'
	 when coalesce(a.empleados, case when coalesce(a.empleados,case when patindex('%[^0-9]%',f.deal_company_employee_q) > 0 then null else cast(f.deal_company_employee_q as bigint) end) > f.company_employee_q
						   then coalesce(a.empleados,case when patindex('%[^0-9]%',f.deal_company_employee_q) > 0 then null else cast(f.deal_company_employee_q as bigint) end)
						   else f.company_employee_q end) < 100 then 'SME'
	 when coalesce(a.empleados, case when coalesce(a.empleados,case when patindex('%[^0-9]%',f.deal_company_employee_q) > 0 then null else cast(f.deal_company_employee_q as bigint) end) > f.company_employee_q
						   then coalesce(a.empleados,case when patindex('%[^0-9]%',f.deal_company_employee_q) > 0 then null else cast(f.deal_company_employee_q as bigint) end)
						   else f.company_employee_q end) < 1000 then 'Mid market'
	 else 'Enterprise' end segment,
case when sum(coalesce(f.licencias_faltante,a.licencias,f.licencias_vendidas,0)) over (partition by f.parent_company_id) = 0 then 'Undefined'
	 when sum(coalesce(f.licencias_faltante,a.licencias,f.licencias_vendidas,0)) over (partition by f.parent_company_id) <= 50 then 'Small'
	 when sum(coalesce(f.licencias_faltante,a.licencias,f.licencias_vendidas,0)) over (partition by f.parent_company_id) <= 100 then 'Medium'
	 else 'Large' end licences_based_size,
f.source,
f.n_mero_de_pagos,
f.plazo_de_pago,
f.tipo_negocio_kam,
f.kam,
f.sdr,
f.account_executive,
f.account_executive_team,
flag_num_pagos,
flag_plazo_de_pago,
coalesce(f.arr_type_faltante,a.arr_type,f.arr_type) arr_type,
f.previousstage,
coalesce(f.bd_faltante, f.bd_compartido, a.bd, f.bd) bd,
planta_final.area team,
coalesce(case when f.arr_type_faltante is null then null else planta_final.pais end, a.pais_ubits, f.pais_ubits) pais_ubits,
planta_final.subteam subteam,
planta_final.subteam_active subteam_active,
coalesce(mgr.manager,'Inactive') manager,
planta_final.cargo_type,
a.flag_arr_type,
f.inmonth,
f.enteredDate_SQL,
f.enteredDate_Opportunity,
f.enteredDate_Upside,
f.enteredDate_Forecast,
f.enteredDate_In,
f.enteredDate_Lost,
f.exitedDate_SQL,
f.ExitedDate_Opportunity,
f.exitedDate_Upside,
f.exitedDate_Forecast,
f.exitedDate_In,
f.StageTime_SQL,
f.StageTime_Opportunity,
f.StageTime_Upside,
f.StageTime_Forecast,
f.StageTime_In,
f.StageTime_Lost,
substring(convert(varchar,coalesce(f.fecha_cambio,a.closedate_modificado,f.finalclosedate),102),1,7) closemonth,
f.first_closed_deal_id,
f.earliest_deal_closedate,
f.first_closed_deal_id_child_company,
f.earliest_deal_closedate_child_company,
f.days_in_ru_to_close,
f.months_since_entry,
f.weeks_since_entry,
f.days_since_entry,
f.actual_months_since_entry,
f.actual_weeks_since_entry,
--
case when datediff(mm,planta_final.ingreso,coalesce(f.fecha_cambio,a.closedate_modificado,f.finalclosedate)) + case when datepart(d,planta_final.ingreso) <= 15 then 1 else 0 end <= 7
	 then datediff(mm,planta_final.ingreso,coalesce(f.fecha_cambio,a.closedate_modificado,f.finalclosedate)) + case when datepart(d,planta_final.ingreso) <= 15 then 1 else 0 end
	 else 7 end months_since_entry_close,
case when datediff(ww,dateadd(dd,-1,planta_final.ingreso),dateadd(dd,-1,coalesce(f.fecha_cambio,a.closedate_modificado,f.finalclosedate))) + 1 <= 12
	 then datediff(ww,dateadd(dd,-1,planta_final.ingreso),dateadd(dd,-1,coalesce(f.fecha_cambio,a.closedate_modificado,f.finalclosedate))) + 1
	 else 13 end weeks_since_entry_close,
datediff(d,planta_final.ingreso,coalesce(f.fecha_cambio,a.closedate_modificado,f.finalclosedate)) + 1 days_since_entry_close,
datediff(mm,planta_final.ingreso,coalesce(f.fecha_cambio,a.closedate_modificado,f.finalclosedate)) + case when datepart(d,planta_final.ingreso) <= 15 then 1 else 0 end actual_months_since_entry_close,
datediff(ww,dateadd(dd,-1,planta_final.ingreso),dateadd(dd,-1,coalesce(f.fecha_cambio,a.closedate_modificado,f.finalclosedate))) + 1 actual_weeks_since_entry_close,
datediff(mm,planta_final.ingreso,getdate()) + case when datepart(d,planta_final.ingreso) <= 15 then 1 else 0 end antiguedad,
planta_final.ingreso,
planta_final.pais_2, planta_final.segmento segmento_planta,
--- dates_opp
opp_date.yyyy opp_yyyy, opp_date.mm opp_mm, opp_date.ww opp_ww, opp_date.week_start opp_week_start,
planta_opp.ingreso opp_ingreso,
case when datediff(mm,planta_opp.ingreso,opp_date.dte) + case when datepart(d,planta_opp.ingreso) <= 15 then 1 else 0 end <= 7
	 then datediff(mm,planta_opp.ingreso,opp_date.dte) + case when datepart(d,planta_opp.ingreso) <= 15 then 1 else 0 end
	 else 7 end months_since_entry_opp,
case when datediff(ww,dateadd(dd,-1,planta_opp.ingreso),dateadd(dd,-1,opp_date.dte)) + 1 <= 12
	 then datediff(ww,dateadd(dd,-1,planta_opp.ingreso),dateadd(dd,-1,opp_date.dte)) + 1
	 else 13 end weeks_since_entry_opp,
datediff(d,planta_opp.ingreso,opp_date.dte) + 1 days_since_entry_opp,
datediff(mm,planta_opp.ingreso,opp_date.dte) + case when datepart(d,planta_opp.ingreso) <= 15 then 1 else 0 end actual_months_since_entry_opp,
datediff(ww,dateadd(dd,-1,planta_opp.ingreso),dateadd(dd,-1,opp_date.dte)) + 1 actual_weeks_since_entry_opp,
datediff(mm,planta_opp.ingreso,getdate()) + case when datepart(d,planta_opp.ingreso) <= 15 then 1 else 0 end antiguedad_opp,
coalesce(cohort_opp.Name,'Unidentified') opp_cohort,
planta_opp.area opp_team,
coalesce(case when f.arr_type_faltante is null then null else planta_opp.pais end, a.pais_ubits, f.pais_ubits) opp_pais_ubits,
planta_opp.pais_2 opp_pais_ubits_2,
planta_opp.subteam opp_subteam,
planta_opp.subteam_active opp_subteam_active,
coalesce(mgr_opp.manager,'Inactive') opp_manager,
planta_opp.cargo_type opp_cargo_type,
/*--
	f.months_since_entry_close,
	f.weeks_since_entry_close,
	f.days_since_entry_close,
*/
a.flag_pais,
a.flag_deal_country,
a.flag_close_date,
a.flag_licencias,
a.flag_empleados,
a.flag_empleados_diligenciados,
datediff(d,f.create_date_val,coalesce(f.fecha_cambio,a.closedate_modificado,f.finalclosedate)) tiempo_ciere,
f.fecha_inicio_prueba_30_dias,
f.fecha_fin_prueba_30_dias,
f.fecha_solicitud_prueba_30_dias,
f.demo_30_days_label,
f.semanas_en_demo_label,
f.fecha_cambio_a_opportunity,
f.sdr_ingreso,
case when datediff(mm,sdr_ingreso,sdr_opp_date.dte) + case when datepart(d,sdr_ingreso) <= 15 then 1 else 0 end <= 7
	 then datediff(mm,sdr_ingreso,sdr_opp_date.dte) + case when datepart(d,sdr_ingreso) <= 15 then 1 else 0 end
	 else 7 end months_since_entry_opp_sdr,
case when datediff(ww,dateadd(dd,-1,sdr_ingreso),dateadd(dd,-1,sdr_opp_date.dte)) + 1 <= 12
	 then datediff(ww,dateadd(dd,-1,sdr_ingreso),dateadd(dd,-1,sdr_opp_date.dte)) + 1
	 else 13 end weeks_since_entry_opp_sdr,
datediff(d,sdr_ingreso,sdr_opp_date.dte) + 1 days_since_entry_opp_sdr,
datediff(mm,sdr_ingreso,sdr_opp_date.dte) + case when datepart(d,sdr_ingreso) <= 15 then 1 else 0 end actual_months_since_entry_opp_sdr,
datediff(ww,dateadd(dd,-1,sdr_ingreso),dateadd(dd,-1,sdr_opp_date.dte)) + 1 actual_weeks_since_entry_opp_sdr,
datediff(mm,sdr_ingreso,getdate()) + case when datepart(d,sdr_ingreso) <= 15 then 1 else 0 end antiguedad_opp_sdr,
row_number() over(partition by f.dealid order by coalesce(f.arr_faltante,a.arr,f.amount_in_home_currency)*coalesce(f.part_compartidos,1), coalesce(f.bd_faltante, f.bd_compartido, a.bd, f.bd)) nombre,
f.deal_currency_code,
f.razon_social,
f.nit
from ( select deals.*, faltantes.arr_type arr_type_faltante, faltantes.arr arr_faltante, faltantes.licencias licencias_faltante, faltantes.bd bd_faltante, faltantes.fecha_cambio, compartidos.bd bd_compartido, compartidos.part part_compartidos
	   from final_data deals
	   left join faltantes on (deals.dealId = faltantes.deal_id)
	   left join [dbo].[ajustes_negocios_compartidos] compartidos on (deals.dealId = compartidos.dealid)
	 ) f
left join [dbo].[ajustes_preliminar_base] a on (f.dealid = a.dealid)
left join [dbo].[ajustes_industry_over_write] i on (f.dealId = i.deal_id)
left join [dbo].[dim_dates] close_dates_final on (cast(coalesce(f.fecha_cambio,a.closedate_modificado,f.finalclosedate) as date) = close_dates_final.dte)
left join [dbo].[dim_dates] sdr_opp_date on (cast(fecha_cambio_a_opportunity as date) = sdr_opp_date.dte)
left join [dbo].[planta_vw] planta_final on (coalesce(f.bd_faltante, f.bd_compartido, a.bd, f.bd) = planta_final.nombre and close_dates_final.dte between planta_final.valid_from and planta_final.valid_to)
left join [dbo].[dim_cohort_comercial] as closing_cohort_final on (planta_final.ingreso between closing_cohort_final.startdate and closing_cohort_final.enddate)
left join [dbo].[manager_list] mgr on (coalesce(f.bd_faltante, f.bd_compartido, a.bd, f.bd) = mgr.nombre and close_dates_final.dte = mgr.dte)
left join [dbo].[dim_dates] opp_date on (cast(f.enteredDate_Opportunity as date) = opp_date.dte)
left join [dbo].[planta_vw] planta_opp on (coalesce(f.bd_faltante, f.bd_compartido, a.bd, f.bd) = planta_opp.nombre and opp_date.dte between planta_opp.valid_from and planta_opp.valid_to)
left join [dbo].[planta_vw] planta_sdr_opp on (f.sdr = planta_sdr_opp.nombre and sdr_opp_date.dte between planta_sdr_opp.valid_from and planta_sdr_opp.valid_to)
left join [dbo].[planta_vw] planta_acc_exe on (f.account_executive = planta_acc_exe.nombre and sdr_opp_date.dte between planta_acc_exe.valid_from and planta_acc_exe.valid_to)
left join [dbo].[dim_cohort_comercial] as cohort_opp on (planta_opp.ingreso between cohort_opp.startdate and cohort_opp.enddate)
left join [dbo].[manager_list] mgr_opp on (coalesce(f.bd_faltante, f.bd_compartido, a.bd, f.bd) = mgr_opp.nombre and opp_date.dte = mgr_opp.dte)
where
(a.ajuste <> 6 or a.ajuste is null)
and (coalesce(a.arr,f.amount_in_home_currency) <> 0 or f.pipeline_name like '%Sales%')
and not (dealname like '%prueba %' and f.dealid not in (9297747729,8444646156))
--and coalesce(f.bd_compartido,f.bd_faltante,a.bd,f.bd) = 'De la Puente Fernandez Rafael' --and coalesce(mgr.manager,'Inactive') = 'Inactive'
--and yyyy =2021 and mm=4'
--and f.parent_company_name like '%bachoco%'
--and industry = 'Unidentified'
--and f.dealid = 9418033170
--and coalesce(f.bd_faltante,a.bd,f.bd) like '%miranda%'
--and coalesce(case when f.arr_type_faltante is null then null else planta_final.pais end, a.pais_ubits, f.pais_ubits) = 'Mexico'
--and f.stage = 'In'
--and f.Demo_count > 0
--and f.dealid in (4850032211,4713056044)
--and f.parent_company_id = 2527140759
--first_deal_created_date != first_deal_createdate
--and (f.Pipeline_count > 0 or f.Demo_count > 0)
--and closemonth = '2019.12'
--bd is null
-- team is null
--order by closedate
--select count(*) from final_data where closedateadjustmentflag = 1
;

--select * from [dbo].[deals_last_snapshot] where dealid in (4850032211,4713056044)
--select * from planta_vw where nombre like '%salazar%'

--delete from [dbo].[Deals_PropertyHistory]
--delete from [dbo].[deals_last_snapshot]