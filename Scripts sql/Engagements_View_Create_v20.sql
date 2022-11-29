use sales;
go

/*

drop view engagement_analysis

select * from engagement_analysis

cambio: se ajustaron los months since entry a 7
V.17. Cambio incluyendo la duracion en segundos y se incluyen los correcos incoming
V.18. Cambio weeks_since_entry tope de 8 a 12

*/

-- create view engagement_analysis as
select
	[engagement.id] id,
	substring([associations.companyIds],1,case when charindex(',',[associations.companyIds]) = 0 then 10000 else charindex(',',[associations.companyIds]) - 1 end) companyId,
	[associations.contactIds] contactIds,
	[engagement.type] type,
	[engagement.activityType] activity_type,
	stamp_date.yyyy,
	stamp_date.mm,
	stamp_date.ww,
	stamp_date.week_start timestamp_week_start,
	stamp_date.dte,
	planta.ingreso,
	datediff(d,planta.ingreso,stamp_date.dte) + 1 days_since_entry,
	case when [engagement.type] = 'MEETING' and datediff(ww,planta.ingreso,stamp_date.dte) + 1 between 1 and 2 then 2
		 when datediff(ww,dateadd(dd,-1,planta.ingreso), dateadd(dd,-1,stamp_date.dte)) + 1 <= 12
		 then datediff(ww,dateadd(dd,-1,planta.ingreso), dateadd(dd,-1,stamp_date.dte)) + 1
		 else 13 end weeks_since_entry,
	case when datediff(mm,planta.ingreso,stamp_date.dte) + case when datepart(d,planta.ingreso) <= 15 then 1 else 0 end <= 7
		 then datediff(mm,planta.ingreso,stamp_date.dte) + case when datepart(d,planta.ingreso) <= 15 then 1 else 0 end
		 else 7 end months_since_entry,
	case when [engagement.type] = 'MEETING' and datediff(ww,planta.ingreso,stamp_date.dte) + 1 between 1 and 2 then 2
		 else datediff(ww,dateadd(dd,-1,planta.ingreso), dateadd(dd,-1,stamp_date.dte)) + 1 end actual_weeks_since_entry,
	datediff(mm,planta.ingreso,stamp_date.dte) + case when datepart(d,planta.ingreso) <= 15 then 1 else 0 end actual_months_since_entry,
	coalesce(stamp_cohort.Name,'Unidentified') stamp_cohort,
	planta.nombre bd, planta.area as team, planta.subteam, planta.subteam_active, planta.pais
	, [engagement.timestamp]
	, dispositions.disposition_label call_outcome
	, [metadata.meetingOutcome] meeting_outcome
	, [metadata.status] engagement_status
	, [metadata.durationMilliseconds]/1000 call_duration
	, createdAt
	, creation_date.yyyy creation_yyyy
	, creation_date.mm creation_mm
	, creation_date.ww creation_ww
	, creation_date.week_start creation_week_start
	, creation_date.dte creation_dte
	, planta_creation.ingreso ingreso_creacion,
	datediff(d,planta_creation.ingreso,creation_date.dte) + 1 creation_days_since_entry,
	case when [engagement.type] = 'MEETING' and datediff(ww,planta_creation.ingreso,creation_date.dte) + 1 between 1 and 2 then 2 -- para citas de primera semana como no hay meta, se cuentan en segunda semana
		 when datediff(ww,dateadd(dd,-1,planta_creation.ingreso), dateadd(dd,-1,creation_date.dte)) + 1 <= 12
		 then datediff(ww,dateadd(dd,-1,planta_creation.ingreso), dateadd(dd,-1,creation_date.dte)) + 1
		 else 13 end creation_weeks_since_entry,
	case when datediff(mm,planta_creation.ingreso,creation_date.dte) + case when datepart(d,planta_creation.ingreso) <= 15 then 1 else 0 end <= 7
		 then datediff(mm,planta_creation.ingreso,creation_date.dte) + case when datepart(d,planta_creation.ingreso) <= 15 then 1 else 0 end
		 else 7 end creation_months_since_entry,
	case when [engagement.type] = 'MEETING' and datediff(ww,planta_creation.ingreso,creation_date.dte) + 1 between 1 and 2 then 2
		 else datediff(ww,dateadd(dd,-1,planta_creation.ingreso), dateadd(dd,-1,creation_date.dte)) + 1 end creation_actual_weeks_since_entry,
	datediff(mm,planta_creation.ingreso,creation_date.dte) + case when datepart(d,planta_creation.ingreso) <= 15 then 1 else 0 end creation_actual_months_since_entry,
	coalesce(creation_cohort.Name,'Unidentified') creation_cohort,
	planta_creation.nombre creation_bd, planta_creation.area as creation_team, planta_creation.subteam creation_subteam, planta_creation.subteam_active creation_subteam_active, planta_creation.pais creation_pais
from [dbo].[engagement_last_snapshot_2] engagements
left join [dbo].[dim_dispositions] dispositions on (engagements.[metadata.disposition] = dispositions.id)
left join [dbo].[owners_last_snapshot] owners on (engagements.[engagement.ownerId] = owners.ownerId) -- ajustar con creation user
left join [dbo].[dim_dates] stamp_date on (cast(engagements.[engagement.timestamp] as date) = stamp_date.dte)
left join [dbo].[planta_vw] planta on (engagements.[engagement.ownerId] = planta.owner_id and stamp_date.dte between planta.valid_from and planta.valid_to) -- ajustar con creation user
left join [dbo].[dim_cohort_comercial] as stamp_cohort on (planta.ingreso between stamp_cohort.startdate and stamp_cohort.enddate)
left join [dbo].[dim_dates] creation_date on (cast(engagements.[engagement.createdAt] as date) = creation_date.dte)
left join [dbo].[planta_vw] planta_creation on (engagements.[engagement.ownerId] = planta_creation.owner_id and creation_date.dte between planta_creation.valid_from and planta_creation.valid_to) -- ajustar con creation user
left join [dbo].[dim_cohort_comercial] as creation_cohort on (planta_creation.ingreso between creation_cohort.startdate and creation_cohort.enddate)
where 
case when [engagement.type] = 'MEETING' then 1
	 when [engagement.type] = 'EMAIL' then 1
	 when [engagement.type] = 'INCOMING_EMAIL' then 1
	 when [engagement.type] = 'TASK' then 1
	 when [engagement.type] = 'CALL' and stamp_date.dte < CAST('2021-06-01' as date) then 1
	 when [engagement.type] = 'CALL' and stamp_date.dte >= CAST('2021-06-01' as date) and dispositions.disposition_label in ('Connected','Left live message','Left voicemail') then 1
	 else 0 end = 1
--and [engagement.id] = 25915421700
--[engagement.type] in ('CALL','MEETING')-- and datediff(ww,planta.ingreso,stamp_date.dte) + 1 = 1
--and planta.nombre = 'Romero Herrera Andrey Nayib'
--and [engagement.type] = 'MEETING'
--and [metadata.meetingOutcome] = 'COMPLETED'
--and cast([engagement.timestamp] as date) between cast('2022-09-26' as date) and cast('2023-02-23' as date)
--and [engagement.ownerId] = 60781494
--AND CAST([engagement.TIMESTAMP] AS DATE) BETWEEN CAST('2021-02-22' AS DATE) AND CAST('2021-02-28' AS DATE)

--order by stamp_date.dte

/*
select * from [dbo].[engagement_last_snapshot] engagements
where [engagement.id] = 1281783824
--engagements.[associations.companyIds] = '1157525658'
engagements.[engagement.type] = 'CALL'
group by [metadata.status]



select * from owners_last_snapshot where ownerid = 33539735




*/
