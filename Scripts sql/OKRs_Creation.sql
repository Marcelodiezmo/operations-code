 -- marcar lunes como primer día de la semana

use sales;
go
set datefirst 1;

/*
v.14. Cambiar "comercial" por "sales"
v.15. Cambiar evaluación semanas hasta 12
*/

drop table desempeno_metas_db;

--create table desempeno_metas_db as
with all_join_since_entry as (
select
	planta.nombre, planta.cargo, planta.area team, planta.subteam, planta.subteam_active, mgr.manager, planta.cargo_type, planta.flag_first_cargo, planta.pais, planta.pais_2, planta.segmento,
	dates.yyyy,dates.mm,dates.ww,dates.week_start, --dates.dte,
	case when datediff(ww,dateadd(dd,-1,planta.ingreso),dateadd(dd,-1,dates.dte)) + 1 <= 12 --and (dates.dte >= CAST('2021-02-01' as date))
		 then datediff(ww,dateadd(dd,-1,planta.ingreso),dateadd(dd,-1,dates.dte)) + 1
		 else 13 end weeks_since_entry,
	case when datediff(ww,dateadd(dd,-1,coalesce(hire_adj.fecha_entrega,case when planta.flag_first_cargo = 1 then planta.ingreso else planta.ingreso_nuevo_cargo end)),dateadd(dd,-1,dates.dte)) + 1 <= 12 --and (dates.dte >= CAST('2021-02-01' as date))
		 then datediff(ww,dateadd(dd,-1,coalesce(hire_adj.fecha_entrega,case when planta.flag_first_cargo = 1 then planta.ingreso else planta.ingreso_nuevo_cargo end)),dateadd(dd,-1,dates.dte)) + 1
		 else 13 end weeks_since_entry_join,
	case when datediff(mm,planta.ingreso,dates.dte) + case when datepart(d,planta.ingreso) <= 15 then 1 else 0 end <= 7
		 then datediff(mm,planta.ingreso,dates.dte) + case when datepart(d,planta.ingreso) <= 15 then 1 else 0 end
		 else 7 end months_since_entry,
	case when datediff(mm,case when planta.flag_first_cargo = 1 then planta.ingreso else planta.ingreso_nuevo_cargo end,dates.dte) + 1 = 1 then 1
		 when (case when (datepart(d,case when planta.flag_first_cargo = 1 then planta.ingreso else planta.ingreso_nuevo_cargo end) <= 15 /*or planta.nombre in ('Anaya Arguello Juan David','Bravo Aguinaga Patricio Andres','Velez Bustamante Veronica')*/) then 1 else 0 end) + datediff(mm,case when planta.flag_first_cargo = 1 then planta.ingreso else planta.ingreso_nuevo_cargo end,dates.dte) <= 7
		 then (case when (datepart(d,case when planta.flag_first_cargo = 1 then planta.ingreso else planta.ingreso_nuevo_cargo end) <= 15 /*or planta.nombre in ('Anaya Arguello Juan David','Bravo Aguinaga Patricio Andres','Velez Bustamante Veronica')*/) then 1 else 0 end) + datediff(mm,case when planta.flag_first_cargo = 1 then planta.ingreso else planta.ingreso_nuevo_cargo end,dates.dte)
		 else 7 end months_since_entry_join,
	datediff(ww,dateadd(dd,-1,planta.ingreso),dateadd(dd,-1,dates.dte)) + 1 actual_weeks_since_entry,
	datediff(mm,planta.ingreso,dates.dte) + case when datepart(d,planta.ingreso) <= 15 then 1 else 0 end actual_months_since_entry,
	case when datediff(mm,planta.ingreso,dates.dte) + 1 = 1 then 1
		 else (case when (datepart(d,planta.ingreso) <= 15 /*or planta.nombre in ('Anaya Arguello Juan David','Bravo Aguinaga Patricio Andres','Velez Bustamante Veronica')*/) then 1 else 0 end) + datediff(mm,planta.ingreso,dates.dte) end actual_months_since_entry_join,
cast(sum(case when datepart(dw,dates.dte) between 1 and 5 then 1 else 0 end) as float)/(5) part_ww,
DATEDIFF(m, planta.ingreso, GETDATE()) AS antiguedad, CAST(planta.activo AS int) AS flag_activo, planta.ingreso,
planta.terminacion, datepart(year,planta.terminacion) termination_year, datepart(month,planta.terminacion) termination_month, datepart(day,planta.terminacion) termination_day
from planta_vw planta
LEFT join dbo.manager_list mgr ON planta.nombre = mgr.nombre and (mgr.dte between planta.valid_from and planta.valid_to)
left join [dbo].[ajustes_ingreso_listas] hire_adj on (planta.nombre = hire_adj.nombre)
left join [dbo].[dim_dates] dates on (dates.dte = mgr.dte)
where
planta.area like '%sales%' --and activo = 1
and dates.yyyy between 2020 and 2022
--and planta.nombre like '%andrey%'
--and metrics = 'Part_New_Citas'
group by
	planta.nombre, planta.cargo, planta.area, planta.subteam, planta.subteam_active, mgr.manager, planta.cargo_type, planta.flag_first_cargo, planta.pais, planta.ingreso, planta.pais_2, planta.segmento,
	dates.yyyy,dates.mm,dates.ww,dates.week_start, --dates.dte,
	case when datediff(ww,dateadd(dd,-1,planta.ingreso),dateadd(dd,-1,dates.dte)) + 1 <= 12 --and (dates.dte >= CAST('2021-02-01' as date))
		 then datediff(ww,dateadd(dd,-1,planta.ingreso),dateadd(dd,-1,dates.dte)) + 1
		 else 13 end,
	case when datediff(ww,dateadd(dd,-1,coalesce(hire_adj.fecha_entrega,case when planta.flag_first_cargo = 1 then planta.ingreso else planta.ingreso_nuevo_cargo end)),dateadd(dd,-1,dates.dte)) + 1 <= 12 --and (dates.dte >= CAST('2021-02-01' as date))
		 then datediff(ww,dateadd(dd,-1,coalesce(hire_adj.fecha_entrega,case when planta.flag_first_cargo = 1 then planta.ingreso else planta.ingreso_nuevo_cargo end)),dateadd(dd,-1,dates.dte)) + 1
		 else 13 end,
	case when datediff(mm,planta.ingreso,dates.dte) + case when datepart(d,planta.ingreso) <= 15 then 1 else 0 end <= 7
		 then datediff(mm,planta.ingreso,dates.dte) + case when datepart(d,planta.ingreso) <= 15 then 1 else 0 end
		 else 7 end,
	case when datediff(mm,case when planta.flag_first_cargo = 1 then planta.ingreso else planta.ingreso_nuevo_cargo end,dates.dte) + 1 = 1 then 1
		 when (case when (datepart(d,case when planta.flag_first_cargo = 1 then planta.ingreso else planta.ingreso_nuevo_cargo end) <= 15 /*or planta.nombre in ('Anaya Arguello Juan David','Bravo Aguinaga Patricio Andres','Velez Bustamante Veronica')*/) then 1 else 0 end) + datediff(mm,case when planta.flag_first_cargo = 1 then planta.ingreso else planta.ingreso_nuevo_cargo end,dates.dte) <= 7
		 then (case when (datepart(d,case when planta.flag_first_cargo = 1 then planta.ingreso else planta.ingreso_nuevo_cargo end) <= 15 /*or planta.nombre in ('Anaya Arguello Juan David','Bravo Aguinaga Patricio Andres','Velez Bustamante Veronica')*/) then 1 else 0 end) + datediff(mm,case when planta.flag_first_cargo = 1 then planta.ingreso else planta.ingreso_nuevo_cargo end,dates.dte)
		 else 7 end,
	datediff(ww,dateadd(dd,-1,planta.ingreso),dateadd(dd,-1,dates.dte)) + 1,
	datediff(mm,planta.ingreso,dates.dte) + case when datepart(d,planta.ingreso) <= 15 then 1 else 0 end,
	case when datediff(mm,planta.ingreso,dates.dte) + 1 = 1 then 1
		 else (case when (datepart(d,planta.ingreso) <= 15 /*or planta.nombre in ('Anaya Arguello Juan David','Bravo Aguinaga Patricio Andres','Velez Bustamante Veronica')*/) then 1 else 0 end) + datediff(mm,planta.ingreso,dates.dte) end,
	DATEDIFF(m, planta.ingreso, GETDATE()), CAST(planta.activo AS int), planta.ingreso,
	planta.terminacion, datepart(year,planta.terminacion), datepart(month,planta.terminacion), datepart(day,planta.terminacion)
) --select * from all_join_since_entry where nombre like '%Andrey%' and yyyy = 2022 /*and mm = 10 */ order by nombre, mm, ww, week_start
, all_metrics_okr as (
select
	base.yyyy, base.mm, base.ww, base.week_start
	, weeks_since_entry, months_since_entry
	, cohort.Name Cohort
	, base.nombre bd, base.team, base.pais, base.subteam, base.subteam_active, base.manager, base.cargo_type
	, antiguedad, flag_activo, base.ingreso
	, metas.metrics metric
		--, metas.objetivo, base.part_ww, ramp.Ramp_Of
	, (case when metas.metrics = 'New ARR' and sum(metas.objetivo * base.part_ww) over (partition by base.yyyy, base.mm, base.nombre, metas.metrics) > 0
		    then (case when base.segmento = 'SMB' then 3500 when base.cargo_type = 'Jr Business Developer' then 8000 else 10000 end)/(sum(metas.objetivo * base.part_ww) over (partition by base.yyyy, base.mm, base.nombre, metas.metrics)) else 1 end)
	  * round(metas.objetivo * base.part_ww * (case when base.weeks_since_entry > 12 and metas.metrics in ('New ARR','Pipeline') then ramp.ramp_of else 1 end),0)
	  * (case when base.yyyy = base.termination_year and base.mm = base.termination_month  and base.termination_day <= 5 and metas.metrics = 'New ARR' then 0
			  when base.yyyy = 2020 and metas.metrics = 'New ARR' and (base.nombre in ('Ujueta Castillo Julian Alfonso','Segura Vasquez Leonardo')) then cast(18 as float)/cast(16 as float)
			  when base.yyyy = 2020 and metas.metrics = 'New ARR' then cast(14 as float)/cast(16 as float)
			  when base.nombre in ('Arciniega del valle Maria Fernanda','Ramirez Restrepo Mariana') and metas.metrics = 'New ARR' and base.yyyy = 2021 and base.mm <= 7 then 0
			  when base.nombre in ('Arciniega del valle Maria Fernanda','Ramirez Restrepo Mariana') and metas.metrics = 'New ARR' and base.yyyy = 2021 and base.mm = 8 then cast(1 as float)/cast(3 as float)
			  when base.nombre in ('Arciniega del valle Maria Fernanda','Ramirez Restrepo Mariana') and metas.metrics = 'New ARR' and base.yyyy = 2021 and base.mm = 9 then cast(1 as float)/cast(2 as float)
			  when base.nombre in ('Arciniega del valle Maria Fernanda','Ramirez Restrepo Mariana') and metas.metrics = 'New ARR' and base.yyyy = 2021 and base.mm = 10 then cast(3 as float)/cast(4 as float)
			  else 1 end) objetivo
	, (case when metas.metrics = 'New ARR' and sum(metas.objetivo * base.part_ww) over (partition by base.yyyy, base.mm, base.nombre, metas.metrics) > 0
		    then (case when base.segmento = 'SMB' then 3500 else 10000 end)/(sum(metas.objetivo * base.part_ww) over (partition by base.yyyy, base.mm, base.nombre, metas.metrics)) else 1 end) nn2
	, round(metas.objetivo * base.part_ww * (case when base.weeks_since_entry > 12 and metas.metrics in ('New ARR','Pipeline') then ramp.ramp_of else 1 end),0) nn
	  /** (case when base.yyyy = 2020 and metas.metrics = 'New ARR' and (base.nombre in ('Ujueta Castillo Julian Alfonso','Segura Vasquez Leonardo')) then cast(18 as float)/cast(16 as float)
			  when base.yyyy = 2020 and metas.metrics = 'New ARR' then cast(14 as float)/cast(16 as float)
			  else 1 end) objetivo*/
	, metas.factor
	, actual_months_since_entry
	, actual_weeks_since_entry
	, part_ww
from all_join_since_entry base
left join [dbo].[consolidado_metas] metas on (base.weeks_since_entry_join = metas.semana and base.cargo_type = metas.cargo and base.pais_2 = metas.pais and base.segmento = metas.segmento and base.flag_first_cargo = metas.first_title)
left join [dbo].[dim_ramp_of_metrics] ramp on (base.months_since_entry_join = ramp.mes)
left join [dbo].[dim_cohort_comercial] cohort on (cast(base.ingreso as date) between cast(cohort.StartDate as date) and cast(cohort.EndDate as date))
where metas.automated = 1
)
select *
into desempeno_metas_db
from all_metrics_okr
where objetivo >= 0
--and bd like '%Andrey%' 
--and ww = 4
--and metric = 'New ARR'
order by week_start



--select * from planta_vw where nombre like '%Fajardo Salazar Edgar Camilo%'