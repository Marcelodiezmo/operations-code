use sales;
go

/*

drop view planta_vw

select * from planta

v.15. Ajustar Expansiones en ventas.
v.16. Ajustar SDR Leads
v.17. Ajustar Sales Manager
v.18. Ajustar Jr BDs

*/

-- create view planta_vw as
with info_partida as (
select
id,
row_number() over(partition by nombre order by valid_from) rank_id,
row_number() over(partition by nombre order by valid_from desc) reverse_rank_id, 
replace(trim(planta.nombre), '  ', ' ') nombre, planta.cargo, planta.gente_a_cargo, planta.area, planta.cco,
replace(trim(planta.direct_report), '  ', ' ') direct_report, planta.pais, planta.ingreso, planta.nombre_hs, planta.owner_id, 
planta.hs_user_id, planta.terminacion, planta.dead, min(cast(planta.activo as int)) over (partition by planta.nombre, planta.vinculacion) activo,
case when row_number() over (partition by planta.nombre order by planta.valid_from) = 1
	 then planta.valid_from
	 else dateadd(d, 1, planta.valid_from) end valid_from,
case when row_number() over (partition by planta.nombre order by planta.valid_from) = 1
	 then cast(DATEADD(mm, DATEDIFF(mm,0,planta.valid_from), 0) as date)
	 else dateadd(d, 1, planta.valid_from) end valid_from_inicio_mes,
planta.valid_to,
case when (planta.cargo like '%leader%' or planta.cargo like '%head%') or (planta.cargo like '%manager%' and planta.cargo not like '%key%')
	 then planta.nombre
	 else planta.direct_report end subteam,
case when (planta.cargo like '%leader%' or planta.cargo like '%head%') or (planta.cargo like '%manager%' and planta.cargo not like '%key%')
	 then case when planta.activo = 0 then 'Inactive' else planta.nombre end
	 else planta.direct_report_final end subteam_active,
case when cargo like '%Chief Revenue Officer%' and area like '%sales%' then 'CSO'
	 when cargo like '%VP Sales%' and area like '%Sales%' then 'Sales VP'
	 when cargo like '%Sales%Manager%' and area like '%Sales%' then 'Sales Manager'
	 when lower(cargo) like '%expansions%' and cargo like '%Head%' then 'Expansions Head'
	 when lower(cargo) like '%expansions%' then 'Expansions Expert'
	 when (cargo like '%Leader%' or cargo like '%Head%') and (cargo like '%Senior%') and area like '%Sales%' and cargo not like '%SDR%' then 'Sr Industry Head'
	 when (cargo like '%Leader%' or cargo like '%Head%') and area like '%Sales%'  and cargo not like '%SDR%' and lower(cargo) not like '%expansions%' then 'Industry Head'
	 when cargo like '%Representative%' and (area like '%Sales%' or area like '%SDR%') and valid_to >= cast('2021-03-01' as date) then 'Sales Developer Representative'
	 when cargo like '%Representative%' and (area like '%Sales%' or area like '%SDR%') and cargo like '%Lead%' then 'Sales Developer Representative Lead'
	 when cargo like '%Representative%' and area like '%growth%' then 'Sales Developer Representative'
	 when cargo like '%SDR%' and cargo like '%Head%' then 'SDR Head'
	 when cargo like '%Senior%' and area like '%Sales%' then 'Sr Business Developer'
	 when cargo like '%Junior%' and area like '%Sales%' then 'Jr Business Developer'
	 when cargo like '%Intern%' and area like '%Sales%' then 'Intern Comercial'
	 when area like '%Sales%' then 'Business Developer'
	 when area like '%customer%' and cargo like '%VP%' then 'Customer Success VP'
	 when area like '%customer%' and cargo in ('Customer Success Senior Manager') then 'Customer Success Sr Manager'
	 when area like '%customer%' and cargo in ('Student Success Manager') then 'Student Success Manager'
	 when area like '%customer%' and cargo in ('Customer Experience Manager') then 'Customer Experience Manager'
	 when area like '%customer%' and cargo in ('Customer Success Manager','Customer Sucess Manager') then 'Customer Success Manager'
	 when area like '%customer%' and cargo like '%Head%' and (upper(cargo) like '%KAM%' or upper(cargo) like '%ACCOUNT%') then 'KAMs Head'
	 when area like '%customer%' and cargo like '%Head%' and (upper(cargo) like '%EXPANSION%') then 'Expansions Head'
	 when area like '%customer%' and cargo like '%Head%' then 'Customer Success Head'
	 when area like '%customer%' and (upper(cargo) like '%SR%' or upper(cargo) like '%SENIOR%') and (upper(cargo) like '%KAM%' or upper(cargo) like '%ACCOUNT%' or upper(cargo) like '%KEY%') then 'Sr KAM'
	 when area like '%customer%' and (upper(cargo) like '%KAM%' or upper(cargo) like '%ACCOUNT%' or upper(cargo) like '%KEY%') then 'KAM'
	 when area like '%customer%' and (upper(cargo) like '%Expansions%') then 'Expansions Expert'
	 --when area like '%customer%' then 'KAM'
	 else 'Other' end cargo_type,
planta.rama_formacion, planta.fecha_nacimiento,
datediff(yy,planta.fecha_nacimiento,getdate()) - case when dateadd(yy,datediff(yy,planta.fecha_nacimiento,getdate()),planta.fecha_nacimiento) > getdate() then 1 else 0 end edad,
planta.genero,
planta.pais_2,
planta.segmento
from [sales].[dbo].[dim_planta_activa] planta
)
select *,
min(valid_from) over(partition by nombre, cargo_type) ingreso_nuevo_cargo,
case when min(valid_from) over(partition by nombre, cargo_type) = ingreso then 1 else 0 end flag_first_cargo
from info_partida
--where area like '%customer%'
/*where case when cargo like '%Chief Revenue Officer%' and area like '%sales%' then 'CSO'
	 when cargo like '%VP Sales%' and area like '%Sales%' then 'Sales VP'
	 when cargo like '%Sales%Manager%' and area like '%Sales%' then 'Sales Manager'
	 when lower(cargo) like '%expansions%' and cargo like '%Head%' then 'Expansions Head'
	 when lower(cargo) like '%expansions%' then 'Expansions Expert'
	 when (cargo like '%Leader%' or cargo like '%Head%') and (cargo like '%Senior%') and area like '%Sales%' and cargo not like '%SDR%' then 'Sr Industry Head'
	 when (cargo like '%Leader%' or cargo like '%Head%') and area like '%Sales%'  and cargo not like '%SDR%' and lower(cargo) not like '%expansions%' then 'Industry Head'
	 when cargo like '%Representative%' and (area like '%Sales%' or area like '%SDR%') and valid_to >= cast('2021-03-01' as date) then 'Sales Developer Representative'
	 when cargo like '%Representative%' and (area like '%Sales%' or area like '%SDR%') and cargo like '%Lead%' then 'Sales Developer Representative Lead'
	 when cargo like '%Representative%' and area like '%growth%' then 'Sales Developer Representative'
	 when cargo like '%SDR%' and cargo like '%Head%' then 'SDR Head'
	 when cargo like '%Senior%' and area like '%Sales%' then 'Sr Business Developer'
	 when cargo like '%Intern%' and area like '%Sales%' then 'Intern Comercial'
	 when area like '%Sales%' then 'Business Developer'
	 when area like '%customer%' and cargo like '%VP%' then 'Customer Success VP'
	 when area like '%customer%' and cargo in ('Customer Success Senior Manager') then 'Customer Success Sr Manager'
	 when area like '%customer%' and cargo in ('Student Success Manager') then 'Student Success Manager'
	 when area like '%customer%' and cargo in ('Customer Experience Manager') then 'Customer Experience Manager'
	 when area like '%customer%' and cargo in ('Customer Success Manager','Customer Sucess Manager') then 'Customer Success Manager'
	 when area like '%customer%' and cargo like '%Head%' and (upper(cargo) like '%KAM%' or upper(cargo) like '%ACCOUNT%') then 'KAMs Head'
	 when area like '%customer%' and cargo like '%Head%' and (upper(cargo) like '%EXPANSION%') then 'Expansions Head'
	 when area like '%customer%' and cargo like '%Head%' then 'Customer Success Head'
	 when area like '%customer%' and (upper(cargo) like '%SR%' or upper(cargo) like '%SENIOR%') and (upper(cargo) like '%KAM%' or upper(cargo) like '%ACCOUNT%' or upper(cargo) like '%KEY%') then 'Sr KAM'
	 when area like '%customer%' and (upper(cargo) like '%KAM%' or upper(cargo) like '%ACCOUNT%' or upper(cargo) like '%KEY%') then 'KAM'
	 when area like '%customer%' and (upper(cargo) like '%Expansions%') then 'Expansions Expert'
	 --when area like '%customer%' then 'KAM'
	 else 'Other' end = 'Sales Manager'*/
order by nombre, valid_from