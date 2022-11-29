use sales;
go



/*

v.9. cambio "comercial" por "sales"
v.10. Include level 6 and 7

*/

drop table manager_list;


with p_last as (
select p1.nombre, p1.cargo, p1.direct_report, p2.nombre n2, p2.cargo c2, p3.nombre n3, p3.cargo c3, p4.nombre n4, p4.cargo c4, p5.nombre n5, p5.cargo c5,
case when (p1.cargo like '%manager%' or (p1.cargo like '%VP Sales%') or (p1.cargo like '%head%' and p1.area not like '%Sales%' and p1.area not like '%growth%')) and p1.cargo not like '%key%' then 1
	 when (p2.cargo like '%manager%' or (p2.cargo like '%VP Sales%' and p1.cargo not like '%manager%') or (p2.cargo like '%head%' and p2.area not like '%Sales%' and p2.area not like '%growth%')) and p2.cargo not like '%key%' then 2
	 when (p3.cargo like '%manager%' or (p3.cargo like '%VP Sales%' and p2.cargo not like '%manager%') or (p3.cargo like '%head%' and p3.area not like '%Sales%' and p3.area not like '%growth%')) and p3.cargo not like '%key%' then 3
	 when (p4.cargo like '%manager%' or (p4.cargo like '%VP Sales%' and p3.cargo not like '%manager%') or (p4.cargo like '%head%' and p4.area not like '%Sales%' and p4.area not like '%growth%')) and p4.cargo not like '%key%' then 4
	 when (p5.cargo like '%manager%' or (p5.cargo like '%VP Sales%' and p4.cargo not like '%manager%') or (p5.cargo like '%head%' and p5.area not like '%Sales%' and p5.area not like '%growth%')) and p5.cargo not like '%key%' then 5
	 when (p6.cargo like '%manager%' or (p6.cargo like '%VP Sales%' and p5.cargo not like '%manager%') or (p6.cargo like '%head%' and p6.area not like '%Sales%' and p6.area not like '%growth%')) and p6.cargo not like '%key%' then 6
	 when (p7.cargo like '%manager%' or (p7.cargo like '%VP Sales%' and p6.cargo not like '%manager%') or (p7.cargo like '%head%' and p7.area not like '%Sales%' and p7.area not like '%growth%')) and p7.cargo not like '%key%' then 7
	 else 0 end it,
case when (p1.cargo like '%manager%' or (p1.cargo like '%VP Sales%') or (p1.cargo like '%head%' and p1.area not like '%Sales%' and p1.area not like '%growth%')) and p1.cargo not like '%key%' and p1.terminacion is null then p1.nombre
	 when (p1.cargo like '%manager%' or (p1.cargo like '%VP Sales%') or (p1.cargo like '%head%' and p1.area not like '%Sales%' and p1.area not like '%growth%')) and p1.cargo not like '%key%' then 'Inactive'
	 when (p2.cargo like '%manager%' or (p2.cargo like '%VP Sales%' and p1.cargo not like '%manager%') or (p2.cargo like '%head%' and p2.area not like '%Sales%' and p2.area not like '%growth%')) and p2.cargo not like '%key%' and p2.terminacion is null then p2.nombre
	 when (p2.cargo like '%manager%' or (p2.cargo like '%VP Sales%' and p1.cargo not like '%manager%') or (p2.cargo like '%head%' and p2.area not like '%Sales%' and p2.area not like '%growth%')) and p2.cargo not like '%key%' then 'Inactive'
	 when (p3.cargo like '%manager%' or (p3.cargo like '%VP Sales%' and p2.cargo not like '%manager%') or (p3.cargo like '%head%' and p3.area not like '%Sales%' and p3.area not like '%growth%')) and p3.cargo not like '%key%' and p3.terminacion is null then p3.nombre
	 when (p3.cargo like '%manager%' or (p3.cargo like '%VP Sales%' and p2.cargo not like '%manager%') or (p3.cargo like '%head%' and p3.area not like '%Sales%' and p3.area not like '%growth%')) and p3.cargo not like '%key%' then 'Inactive'
	 when (p4.cargo like '%manager%' or (p4.cargo like '%VP Sales%' and p3.cargo not like '%manager%') or (p4.cargo like '%head%' and p4.area not like '%Sales%' and p4.area not like '%growth%')) and p4.cargo not like '%key%' and p4.terminacion is null then p4.nombre
	 when (p4.cargo like '%manager%' or (p4.cargo like '%VP Sales%' and p3.cargo not like '%manager%') or (p4.cargo like '%head%' and p4.area not like '%Sales%' and p4.area not like '%growth%')) and p4.cargo not like '%key%' then 'Inactive'
	 when (p5.cargo like '%manager%' or (p5.cargo like '%VP Sales%' and p4.cargo not like '%manager%') or (p5.cargo like '%head%' and p5.area not like '%Sales%' and p5.area not like '%growth%')) and p5.cargo not like '%key%' and p5.terminacion is null then p5.nombre
	 when (p5.cargo like '%manager%' or (p5.cargo like '%VP Sales%' and p4.cargo not like '%manager%') or (p5.cargo like '%head%' and p5.area not like '%Sales%' and p5.area not like '%growth%')) and p5.cargo not like '%key%' then 'Inactive'
	 when (p6.cargo like '%manager%' or (p6.cargo like '%VP Sales%' and p5.cargo not like '%manager%') or (p6.cargo like '%head%' and p6.area not like '%Sales%' and p6.area not like '%growth%')) and p6.cargo not like '%key%' and p6.terminacion is null then p6.nombre
	 when (p6.cargo like '%manager%' or (p6.cargo like '%VP Sales%' and p5.cargo not like '%manager%') or (p6.cargo like '%head%' and p6.area not like '%Sales%' and p6.area not like '%growth%')) and p6.cargo not like '%key%' then 'Inactive'
	 when (p7.cargo like '%manager%' or (p7.cargo like '%VP Sales%' and p6.cargo not like '%manager%') or (p7.cargo like '%head%' and p7.area not like '%Sales%' and p7.area not like '%growth%')) and p7.cargo not like '%key%' and p7.terminacion is null then p7.nombre
	 when (p7.cargo like '%manager%' or (p7.cargo like '%VP Sales%' and p6.cargo not like '%manager%') or (p7.cargo like '%head%' and p7.area not like '%Sales%' and p7.area not like '%growth%')) and p7.cargo not like '%key%' then 'Inactive'
	 else 'Other' end manager
from planta_vw p1
left join planta_vw p2 on (replace(trim(p1.direct_report),'  ',' ') = replace(trim(p2.nombre),'  ',' ') and p1.reverse_rank_id = 1 and p2.reverse_rank_id = 1)
left join planta_vw p3 on (replace(trim(p2.direct_report),'  ',' ') = replace(trim(p3.nombre),'  ',' ') and p2.reverse_rank_id = 1 and p3.reverse_rank_id = 1)
left join planta_vw p4 on (replace(trim(p3.direct_report),'  ',' ') = replace(trim(p4.nombre),'  ',' ') and p3.reverse_rank_id = 1 and p4.reverse_rank_id = 1)
left join planta_vw p5 on (replace(trim(p4.direct_report),'  ',' ') = replace(trim(p5.nombre),'  ',' ') and p4.reverse_rank_id = 1 and p5.reverse_rank_id = 1)
left join planta_vw p6 on (replace(trim(p5.direct_report),'  ',' ') = replace(trim(p6.nombre),'  ',' ') and p5.reverse_rank_id = 1 and p6.reverse_rank_id = 1)
left join planta_vw p7 on (replace(trim(p6.direct_report),'  ',' ') = replace(trim(p7.nombre),'  ',' ') and p6.reverse_rank_id = 1 and p7.reverse_rank_id = 1)
where p1.reverse_rank_id = 1
), open_plant as (
	select p1.*, dim_dates.*, p_last.direct_report last_report
	from planta_vw p1
	left join dim_dates on (dte between valid_from and case when valid_to >= cast(cast(datepart(yy,GETDATE())+1 as nvarchar)+'-01-01' as date) then cast(cast(datepart(yy,GETDATE())+2 as nvarchar)+'-01-01' as date) else valid_to end)
	left join planta_vw p_last on (p1.direct_report = p_last.nombre and p_last.reverse_rank_id = 1)
	--where p1.nombre like '%Melo Zambrano Julian David%' and dte = CAST('2021-03-31' as date)
) --select nombre, dte, COUNT(*) from open_plant group by nombre, dte having COUNT(*) > 1
, planta_manager as(
select p1.nombre, p1.dte, p1.area, p1.direct_report, p1.cargo, p1.cargo_type, p1.pais_2, p1.segmento, p1.activo, p2_last.cargo direct_report_title,
p2.nombre n2, p2.dte d2, p2.cargo_type cargo_type_2,
p3.nombre n3, p3.dte d3, p3.cargo_type cargo_type_3,
p4.nombre n4, p4.dte d4, p4.cargo_type cargo_type_4,
p5.nombre n5, p5.dte d5, p5.cargo_type cargo_type_5,
p6.nombre n6, p6.dte d6, p6.cargo_type cargo_type_6,
p7.nombre n7, p7.dte d7, p7.cargo_type cargo_type_7,
case when (p1.cargo like '%manager%' or (p1.cargo like '%VP Sales%') or (p1.cargo like '%head%' and p1.area not like '%Sales%' and p1.area not like '%growth%')) and p1.cargo not like '%key%' then 1
	 when (p2.cargo like '%manager%' or (p2.cargo like '%VP Sales%' and p1.cargo not like '%manager%') or (p2.cargo like '%head%' and p2.area not like '%Sales%' and p2.area not like '%growth%')) and p2.cargo not like '%key%' then 2
	 when (p3.cargo like '%manager%' or (p3.cargo like '%VP Sales%' and p2.cargo not like '%manager%') or (p3.cargo like '%head%' and p3.area not like '%Sales%' and p3.area not like '%growth%')) and p3.cargo not like '%key%' then 3
	 when (p4.cargo like '%manager%' or (p4.cargo like '%VP Sales%' and p3.cargo not like '%manager%') or (p4.cargo like '%head%' and p4.area not like '%Sales%' and p4.area not like '%growth%')) and p4.cargo not like '%key%' then 4
	 when (p5.cargo like '%manager%' or (p5.cargo like '%VP Sales%' and p4.cargo not like '%manager%') or (p5.cargo like '%head%' and p5.area not like '%Sales%' and p5.area not like '%growth%')) and p5.cargo not like '%key%' then 5
	 when (p6.cargo like '%manager%' or (p6.cargo like '%VP Sales%' and p5.cargo not like '%manager%') or (p6.cargo like '%head%' and p6.area not like '%Sales%' and p6.area not like '%growth%')) and p6.cargo not like '%key%' then 6
	 when (p7.cargo like '%manager%' or (p7.cargo like '%VP Sales%' and p6.cargo not like '%manager%') or (p7.cargo like '%head%' and p7.area not like '%Sales%' and p7.area not like '%growth%')) and p7.cargo not like '%key%' then 7
	 else 0 end it,
case when (p1.cargo like '%manager%' or (p1.cargo like '%VP Sales%') or (p1.cargo like '%head%' and p1.area not like '%Sales%' and p1.area not like '%growth%')) and p1.cargo not like '%key%' then p1.nombre
	 when (p2.cargo like '%manager%' or (p2.cargo like '%VP Sales%' and p1.cargo not like '%manager%') or (p2.cargo like '%head%' and p2.area not like '%Sales%' and p2.area not like '%growth%')) and p2.cargo not like '%key%' then p2.nombre
	 when (p3.cargo like '%manager%' or (p3.cargo like '%VP Sales%' and p2.cargo not like '%manager%') or (p3.cargo like '%head%' and p3.area not like '%Sales%' and p3.area not like '%growth%')) and p3.cargo not like '%key%' then p3.nombre
	 when (p4.cargo like '%manager%' or (p4.cargo like '%VP Sales%' and p3.cargo not like '%manager%') or (p4.cargo like '%head%' and p4.area not like '%Sales%' and p4.area not like '%growth%')) and p4.cargo not like '%key%' then p4.nombre
	 when (p5.cargo like '%manager%' or (p5.cargo like '%VP Sales%' and p4.cargo not like '%manager%') or (p5.cargo like '%head%' and p5.area not like '%Sales%' and p5.area not like '%growth%')) and p5.cargo not like '%key%' then p5.nombre
	 when (p6.cargo like '%manager%' or (p6.cargo like '%VP Sales%' and p5.cargo not like '%manager%') or (p6.cargo like '%head%' and p6.area not like '%Sales%' and p6.area not like '%growth%')) and p6.cargo not like '%key%' then p6.nombre
	 when (p7.cargo like '%manager%' or (p7.cargo like '%VP Sales%' and p6.cargo not like '%manager%') or (p7.cargo like '%head%' and p7.area not like '%Sales%' and p7.area not like '%growth%')) and p7.cargo not like '%key%' then p7.nombre
	 else coalesce(p2_last.manager,'Inactive') end manager,
	 p1.ingreso,
	 p1.ingreso_nuevo_cargo,
	 p1.flag_first_cargo
from open_plant p1
left join open_plant p2 on (replace(trim(p1.direct_report),'  ',' ') = replace(trim(p2.nombre),'  ',' ') and p1.dte = p2.dte)
left join p_last p2_last on (replace(trim(p1.direct_report),'  ',' ') = replace(trim(p2_last.nombre),'  ',' '))
left join open_plant p3 on (replace(trim(p2.direct_report),'  ',' ') = replace(trim(p3.nombre),'  ',' ') and p2.dte = p3.dte)
left join open_plant p4 on (replace(trim(p3.direct_report),'  ',' ') = replace(trim(p4.nombre),'  ',' ') and p3.dte = p4.dte)
left join open_plant p5 on (replace(trim(p4.direct_report),'  ',' ') = replace(trim(p5.nombre),'  ',' ') and p4.dte = p5.dte)
left join open_plant p6 on (replace(trim(p5.direct_report),'  ',' ') = replace(trim(p6.nombre),'  ',' ') and p5.dte = p6.dte)
left join open_plant p7 on (replace(trim(p6.direct_report),'  ',' ') = replace(trim(p7.nombre),'  ',' ') and p6.dte = p7.dte)
) select *
into manager_list
from planta_manager p 
--where nombre like '%Parra Valdes Daniel Francisco%'
order by nombre, dte
--where manager = 'Diaz Hernandez Julio Cesar'-- and dte between CAST('2021-04-01' as date) and CAST('2021-04-30' as date)
--where dte = CAST('2022-03-03' as date) order by nombre

--select * from manager_list where dte = CAST(getdate() as date) and area like '%sales%' order by dte, nombre