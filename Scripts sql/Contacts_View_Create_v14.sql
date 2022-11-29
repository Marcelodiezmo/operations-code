use sales;
go

--select @@DATEFIRST
--SET DATEFIRST 1
--select @@DATEFIRST

/*

drop view contacts_analysis

select * from contacts_analysis

se actualiza el months_since_entry
v.14. Se cambia tope de weeks_since_entry a 12
v.15.Include_MQL_SOURCE
*/

-- create view contacts_analysis as

select
	contacts.vid,
	contacts.associatedcompanyid companyId,
	adj_creation_date_presentar.yyyy,
	adj_creation_date_presentar.mm,
	adj_creation_date_presentar.ww,
	adj_creation_date_presentar.week_start creation_week_start,
	adj_creation_date_presentar.dte,
	creation_date.yyyy original_yyyy,
	creation_date.mm original_mm,
	creation_date.ww original_ww,
	creation_date.week_start original_creation_week_start,
	--DATEPART(dw,creation_date.dte) original_dow, @@DATEFIRST,
	creation_date.dte original_dte,
	planta.ingreso,
	datediff(d,planta.ingreso,adj_creation_date_presentar.dte) + 1 days_since_entry,
	case when datediff(ww,dateadd(dd,-1,planta.ingreso), dateadd(dd,-1,adj_creation_date_presentar.dte)) + 1 <= 12
		 then datediff(ww,dateadd(dd,-1,planta.ingreso), dateadd(dd,-1,adj_creation_date_presentar.dte)) + 1
		 else 13 end weeks_since_entry,
	case when datediff(mm,planta.ingreso,adj_creation_date_presentar.dte) + case when datepart(d,planta.ingreso) <= 15 then 1 else 0 end <= 7
		 then datediff(mm,planta.ingreso,adj_creation_date_presentar.dte) + case when datepart(d,planta.ingreso) <= 15 then 1 else 0 end
		 else 7 end months_since_entry,
	datediff(d,planta.ingreso,creation_date.dte) + 1 original_days_since_entry,
	case when datediff(ww,planta.ingreso,creation_date.dte) + 1 <= 12
		 then datediff(ww,planta.ingreso,creation_date.dte) + 1
		 else 13 end original_weeks_since_entry,
	case when datediff(mm,planta.ingreso,creation_date.dte) + case when datepart(d,planta.ingreso) <= 15 then 1 else 0 end <= 7
		 then datediff(mm,planta.ingreso,creation_date.dte) + case when datepart(d,planta.ingreso) <= 15 then 1 else 0 end
		 else 7 end original_months_since_entry,
	datediff(ww,dateadd(dd,-1,planta.ingreso), dateadd(dd,-1,adj_creation_date_presentar.dte)) + 1 actual_weeks_since_entry,
	datediff(mm,planta.ingreso,adj_creation_date_presentar.dte) + case when datepart(d,planta.ingreso) <= 15 then 1 else 0 end actual_months_since_entry,
	datediff(ww,planta.ingreso,creation_date.dte) + 1 actual_original_weeks_since_entry,
	datediff(mm,planta.ingreso,creation_date.dte) + case when datepart(d,planta.ingreso) <= 15 then 1 else 0 end actual_original_months_since_entry,
	coalesce(creation_cohort.Name,'Unidentified') creation_cohort,
	planta.nombre bd, planta.area as team, planta.subteam, subteam_active, planta.pais, manager_list.manager, planta.pais_2, planta.segmento,
	contacts.fuente_mql_label, case when contacts.notes_last_contacted <= dateadd(day,5,creation_date.dte) then 1 else 0 end conteo_comunicacion, 1 contacto_creado,
	case when companies.cosmos_row_number is not null then 1 else 0 end has_row_number,
	contacts.fuente_del_lead_gen_label
from [dbo].[contacts_last_snapshot] contacts
left join [dbo].[companies_last_snapshot] companies on (contacts.associatedcompanyid = companies.companyId)
left join [dbo].[owners_last_snapshot] owners on (contacts.hubspot_owner_id = owners.ownerId) -- ajustar con creation user
left join [dbo].[dim_dates] creation_date on (cast(contacts.createdate as date) = creation_date.dte)
left join planta_vw planta on (contacts.hubspot_owner_id = planta.owner_id and creation_date.dte between planta.valid_from and planta.valid_to) -- ajustar con creation user
left join [dbo].[dim_dates] adj_creation_date on (cast(case when datediff(ww,planta.ingreso,dateadd(d,-1,creation_date.dte)) + 1 in (1,4) then dateadd(dd,6,creation_date.dte)
															when datediff(ww,planta.ingreso,dateadd(d,-1,creation_date.dte)) + 1 = 3 then dateadd(dd,13,creation_date.dte)
															else dateadd(dd,-1,creation_date.dte) end as date) = adj_creation_date.dte)
left join [dbo].[dim_dates] adj_creation_date_presentar on (dateadd(d,1,adj_creation_date.dte) = adj_creation_date_presentar.dte)
left join [dbo].[manager_list] manager_list on (planta.nombre = manager_list.nombre and adj_creation_date_presentar.dte = manager_list.dte)
left join [dbo].[dim_cohort_comercial] as creation_cohort on (planta.ingreso between creation_cohort.startdate and creation_cohort.enddate)
--where planta.nombre like '%Baron%'
--	  and adj_creation_date_presentar.yyyy = 2021 and
--	  case when datediff(ww,dateadd(dd,-1,planta.ingreso), dateadd(dd,-1,adj_creation_date.dte)) + 1 <= 8
--		   then datediff(ww,dateadd(dd,-1,planta.ingreso), dateadd(dd,-1,adj_creation_date.dte)) + 1
--		   else 9 end in (4,5)
--order by creation_date.dte