print('SkynetRulez')

from pyspark.sql.window import Window
from pyspark import StorageLevel
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as f
import numpy as np
import datetime
from datetime import datetime
from datetime import timedelta
from pyspark.sql import SparkSession
from pyspark import StorageLevel
from time import time
from pyspark.sql.functions import sequence, to_date, explode, col
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

spark = SparkSession.builder.appName("operaciones").getOrCreate()
#spark.conf.set("spark.sql.session.timeZone", "UTC-5")
sc = spark.sparkContext


##en local
# import skynetmodule as skm
# environment='prod'
# keyId, keyAccess = skm.get_default_credentials(environment)
# spark, sc = skm.pyspark_local_start(keyId, keyAccess, session_name = "MlUbits")
# spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
############

bucket='strategy-ops/DB/strategy-ops-google-sheets/tablas_dimensiones/'


#read data
nueva_planta = spark.read.format("parquet").option("tempdir",'s3n://'+bucket).option('header','true').option("delimiter", "|").option("partitionOverwriteMode", "dynamic").load('s3n://'+bucket+'nueva_planta.parquet').persist(StorageLevel.DISK_ONLY_2)
nueva_planta_terminacion=spark.read.format("parquet").option("tempdir",'s3n://'+bucket).option('header','true').option("delimiter", "|").option("partitionOverwriteMode", "dynamic").load('s3n://'+bucket+'nueva_planta_terminacion.parquet').persist(StorageLevel.DISK_ONLY_2)
# dim_planta_activa=spark.read.format("parquet").option("tempdir",'s3n://'+bucket).option('header','true').option("delimiter", "|").option("partitionOverwriteMode", "dynamic").load('s3n://'+bucket+'dim_planta_activa.parquet').persist(StorageLevel.DISK_ONLY_2)

try:
    dim_planta_activa=spark.read.format("parquet").option("tempdir",'s3n://'+bucket).option('header','true').option("delimiter", "|").option("partitionOverwriteMode", "dynamic").load('s3n://'+bucket+'/output/dim_planta_activa.parquet').persist(StorageLevel.DISK_ONLY_2)
    print("entró al historico")
except:
    dim_planta_activa=spark.read.format("parquet").option("tempdir",'s3n://'+bucket).option('header','true').option("delimiter", "|").option("partitionOverwriteMode", "dynamic").load('s3n://'+bucket+'dim_planta_activa.parquet').persist(StorageLevel.DISK_ONLY_2)
    print("primera ejecucion")



# string to int
dim_planta_activa=dim_planta_activa.withColumn("gente_a_cargo",dim_planta_activa.gente_a_cargo.cast('int'))
dim_planta_activa=dim_planta_activa.withColumn("vinculacion",dim_planta_activa.vinculacion.cast('int'))
dim_planta_activa=dim_planta_activa.withColumn("activo",dim_planta_activa.activo.cast('int'))
dim_planta_activa=dim_planta_activa.withColumn("id",dim_planta_activa.id.cast('int'))
nueva_planta=nueva_planta.withColumn("NUMERO_DE_IDENTIFICACION",nueva_planta.NUMERO_DE_IDENTIFICACION.cast('int'))
#delete spaces
dim_planta_activa=dim_planta_activa.withColumn("gente_a_cargo",trim(dim_planta_activa.gente_a_cargo))

#max id dim_planta
idmax=dim_planta_activa.agg({'id':'max'}).collect()[0][0]
print(idmax)

#orden columns
ordercolumns=dim_planta_activa.drop('direct_report_final_calc').columns

#columnas que no se sacan en algun lado (estas columnas quedaran siempre nulls)
noColumnsMapeadas=dim_planta_activa.select('id','owner_id','hs_user_id','rama_formacion','pais_2','segmento','nombre_hs','hs_user_id','direct_report_final','formacion','rama_formacion','fecha_nacimiento')

dim_planta_activa.createTempView('dim_planta_activa')
nueva_planta.createTempView('nueva_planta')

#fecha de hoy
getdate=datetime.today().strftime('%Y-%m-%d')
getdatetime=datetime.today().strftime('%Y-%m-%d %H:%M:%S')

#se agrega campos que esten mapeados aqui



#parte 1
joinPlanta=spark.sql(f'''
select
b.NUMERO_DE_IDENTIFICACION as id,
replace(trim(b.Nombre_empleado),'  ',' ') as nombre ,
b.Cargo as cargo,
case when b.Gente_a_cargo = 'No' then 0 else 1 end as gente_a_cargo,
translate(b.Area,'�����','aeiou') as area,
translate(b.Centro_de_Costo,'�����','aeiou') as cco,
replace(trim(b.Jefe_directo),'  ',' ') as direct_report,
b.Pais as pais,
b.Genero as genero,
b.Fecha_de_ingreso_ as ingreso,
1 as activo,
b.Fecha_de_terminacion as terminacion,
0 as dead,
b.Fecha_de_ingreso_ as valid_from,
cast('2100-12-31' as date) as valid_to,
--{getdate} as updatedAt,
1 as vinculacion


from dim_planta_activa as a right join 
nueva_planta b on  (a.nombre = replace(trim(b.Nombre_empleado),'  ',' ') )
where a.nombre is null
order by b.fecha_de_ingreso_;
''')

joinPlanta=joinPlanta.withColumn("updatedAt",current_timestamp())
joinPlanta=joinPlanta.join(noColumnsMapeadas,on='id',how='left')

#max id dim_planta
windowSpec  = Window.orderBy("id")
joinPlanta=joinPlanta.withColumn("row_number",row_number().over(windowSpec))
joinPlanta=joinPlanta.withColumn("id",joinPlanta.row_number+idmax)
joinPlanta=joinPlanta.select(ordercolumns)

dim_planta_activa_nuevo=joinPlanta.union(dim_planta_activa)


 #parte 2
#se tiene la informacion a actualizar
dim_planta_activa_nuevo.createTempView('dim_planta_activa_nuevo')

dataupdate=spark.sql('''
select 
id,
dead,
valid_to
from dim_planta_activa_nuevo as a right join 
nueva_planta b on  (a.nombre = replace(trim(b.Nombre_empleado),'  ',' ') )
where a.nombre is not null
and dead = 0
and (replace(trim(a.cargo),'  ',' ') <> replace(trim(b.cargo),'  ',' ')
or a.gente_a_cargo <> case when b.Gente_a_cargo = 'No' then 0 else 1 end
or translate(a.area,'�����','aeiou') <> translate(b.area,'�����','aeiou')
or translate(a.cco,'�����','aeiou') <> translate(b.centro_de_costo,'�����','aeiou')
or a.direct_report <> replace(trim(b.Jefe_directo),'  ',' ')
or a.pais <> b.pais
or a.genero <> b.genero
or a.terminacion <> b.Fecha_de_terminacion)
and a.activo = 1
'''
)


#se crea columnas con la info a actualizar
dataupdate=dataupdate.withColumn("dead1",lit(1))
dataupdate=dataupdate.withColumn("valid_to1",lit(getdate))
                                            
#se cruza la info que se tiene que actualizar  a la info planta
dataupdate1=dim_planta_activa_nuevo.join(dataupdate.drop('dead','valid_to'),on='id',how='left')

#se actualiza la informacion que corresponda
dataupdate1=dataupdate1.withColumn("dead",when((dataupdate1.dead1.isNull()), lit(dataupdate1.dead)).otherwise(lit(dataupdate1.dead1)))
dataupdate1=dataupdate1.withColumn("valid_to",when((dataupdate1.valid_to1.isNull()), lit(dataupdate1.valid_to)).otherwise(lit(dataupdate1.valid_to1)))
#se elimina columnas de ayuda para la actualizacion
dim_planta_activa_nuevo=dataupdate1.drop('dead1','valid_to1')



# for colname in nueva_planta.columns:
#     nueva_planta = nueva_planta.withColumn(colname, f.trim(f.col(colname)))

#parte 3 #validar este script
dim_planta_activa_nuevo.createTempView('dim_planta_activa_nuevo1')

forinserdata=spark.sql(f'''

select replace(trim(b.Nombre_empleado),'  ',' ') as nombre,

b.Cargo as cargo,
case when b.Gente_a_cargo = "No" then 0 else 1 end as gente_a_cargo,
translate(b.Area,'�����','aeiou') area,
translate(b.Centro_de_Costo,'�����','aeiou') as cco,
replace(trim(b.Jefe_directo),'  ',' ') as direct_report,
b.Pais as pais,
b.Genero as genero,
a.min_ingreso as ingreso,
1 as activo,
b.Fecha_de_terminacion as terminacion,
0 as dead,
--{getdate} as valid_from,
cast('2100-12-31' as date) as valid_to,
max(a.nombre_hs) as nombre_hs,
max(a.owner_id) as owner_id,
max(a.hs_user_id)as hs_user_id,
--{getdate} as updatedAt,
a.max_vinculacion as vinculacion
from (select *, min(cast(dead as int)) over (partition by nombre) min_dead, 
min(cast(ingreso as date)) over (partition by nombre) min_ingreso,
max(cast(vinculacion as int)) over (partition by nombre) max_vinculacion 
from dim_planta_activa_nuevo1) a
right join nueva_planta b on (a.nombre = replace(trim(b.Nombre_empleado),'  ',' '))
where a.nombre is not null
and a.min_dead = 1
and (a.cargo <> b.cargo
or a.gente_a_cargo <> case when b.Gente_a_cargo = 'No' then 0 else 1 end
or a.area <> b.area
or a.cco <> b.centro_de_costo
or a.direct_report <> replace(trim(b.Jefe_directo),'  ',' ')
or a.pais <> b.pais
or a.genero <> b.genero
or a.terminacion <> b.Fecha_de_terminacion)
--and nombre like '%catalina%'
group by
 replace(trim(b.Nombre_empleado),'  ',' '),
b.Cargo,case when b.Gente_a_cargo = 'No' then 0 else 1 end,
b.Area,
b.Centro_de_Costo,
b.Jefe_directo,
b.Pais,
b.Genero,
a.min_ingreso,
b.Fecha_de_terminacion, 
a.max_vinculacion

''')
forinserdata=forinserdata.withColumn("valid_from",current_date())
forinserdata=forinserdata.withColumn("updatedAt",current_timestamp())

dim_planta_activa_nuevo_deleteDrops=dim_planta_activa_nuevo.drop(
 'cargo',
 'gente_a_cargo',
 'area',
 'cco',
 'direct_report',
 'pais',
 'genero',
 'ingreso',
 'nombre_hs',
 'owner_id',
 'activo',
 'hs_user_id',
 'terminacion',
 'dead',
 'valid_from',
 'valid_to',
 #'updatedAt',
 'vinculacion')
lastInsertForName=dim_planta_activa_nuevo_deleteDrops.groupby('nombre').agg({"updatedAt": "max"})
lastInsertForName=lastInsertForName.withColumnRenamed('max(updatedAt)','updatedAt')
lastInsertForName=lastInsertForName.join(dim_planta_activa_nuevo_deleteDrops,on=['nombre','updatedAt'],how='inner').drop('id','updatedAt')


dim_planta_activa_nuevo_2=forinserdata.join(lastInsertForName,on=['nombre'],how='inner')
#max id dim_planta
idmax=dim_planta_activa_nuevo.agg({'id':'max'}).collect()[0][0]
print(idmax)
windowSpec  = Window.orderBy("nombre")

dim_planta_activa_nuevo_2=dim_planta_activa_nuevo_2.withColumn("row_number",row_number().over(windowSpec))
dim_planta_activa_nuevo_2=dim_planta_activa_nuevo_2.withColumn("id",dim_planta_activa_nuevo_2.row_number+idmax)
dim_planta_activa_nuevo_2=dim_planta_activa_nuevo_2.select(ordercolumns)

dim_planta_activa_nuevo_3=dim_planta_activa_nuevo_2.select(ordercolumns).unionAll(dim_planta_activa_nuevo.select(ordercolumns)).distinct()



#parte 4
dim_planta_activa_nuevo_3.createTempView('dim_planta_activa_nuevo_3')

nombreNotLike="Riveros Ospina Manuela"
update_planta_activa=spark.sql(f'''
select
id,
dead,
valid_to,
activo,
updatedAt

from dim_planta_activa_nuevo_3 a
left join nueva_planta b on (a.nombre = replace(trim(b.Nombre_empleado),'  ',' '))
where b.Nombre_empleado is null and a.activo = 1 and dead = 0
and nombre not like '{nombreNotLike}'
'''
)

#se crea columnas con la info a actualizar
update_planta_activa=update_planta_activa.withColumn("dead1",lit(0))
update_planta_activa=update_planta_activa.withColumn("valid_to1",lit(getdate))
update_planta_activa=update_planta_activa.withColumn("activo1",lit(0))


update_planta_activa=update_planta_activa.withColumn("updatedAt1",lit(getdatetime))

                           
#se cruza la info que se tiene que actualizar  a la info planta
dataupdate1=dim_planta_activa_nuevo_3.join(update_planta_activa.drop('dead','valid_to','updatedAt','activo'),on='id',how='left')


#se actualiza la informacion que corresponda
dataupdate1=dataupdate1.withColumn("dead",when((dataupdate1.dead1.isNull()), lit(dataupdate1.dead)).otherwise(lit(dataupdate1.dead1)))
dataupdate1=dataupdate1.withColumn("valid_to",when((dataupdate1.valid_to1.isNull()), lit(dataupdate1.valid_to)).otherwise(lit(dataupdate1.valid_to1)))
dataupdate1=dataupdate1.withColumn("activo",when((dataupdate1.activo1.isNull()), lit(dataupdate1.activo)).otherwise(lit(dataupdate1.activo1)))
dataupdate1=dataupdate1.withColumn("updatedAt",when((dataupdate1.updatedAt1.isNull()), lit(dataupdate1.updatedAt)).otherwise(lit(dataupdate1.updatedAt1)))
#se elimina columnas de ayuda para la actualizacion
dim_planta_activa_nuevo_4=dataupdate1.drop('dead1','valid_to1','updatedAt1','activo1')

                 # parte 5
dim_planta_activa_nuevo_4.createTempView('dim_planta_activa_nuevo4')
nueva_planta_terminacion.createTempView('nueva_planta_terminacion')
update_planta_activa5=spark.sql(f'''
select 
id,
terminacion,
FECHA_DE_TERMINACION as fecha_de_terminacion

from dim_planta_activa_nuevo4  a
inner join nueva_planta_terminacion b on (a.nombre = replace(trim(b.NOMBRE_EMPLEADO),'  ',' '))
where (a.terminacion is null and a.activo = 0) or (a.terminacion='NULL' and a.activo = 0)
'''
)

#se crea columnas con la info a actualizar
update_planta_activa5=update_planta_activa5.withColumn("terminacion1",lit(update_planta_activa5.fecha_de_terminacion))
                      
#se cruza la info que se tiene que actualizar  a la info planta
dataupdate1=dim_planta_activa_nuevo_4.join(update_planta_activa5.drop('terminacion','fecha_de_terminacion'),on='id',how='left')

#se actualiza la informacion que corresponda
dataupdate1=dataupdate1.withColumn("terminacion",when((dataupdate1.terminacion1.isNull()), lit(dataupdate1.terminacion)).otherwise(lit(dataupdate1.terminacion1)))

#se elimina columnas de ayuda para la actualizacion
dim_planta_activa_nuevo_5=dataupdate1.drop('terminacion1')




     #parte 6
dim_planta_activa_nuevo_5.createTempView('dim_planta_activa_nuevo5')
update_planta_activa_6=spark.sql(f'''
select 
id,
b.min_activo,
b.max_terminacion,
a.valid_from,
a.valid_to

from dim_planta_activa_nuevo5 a
left join 
(select nombre, vinculacion, min(cast(activo as int)) min_activo, max(terminacion) max_terminacion 
from dim_planta_activa_nuevo5
where terminacion!='NULL'
group by nombre,vinculacion) b 
on (a.nombre = b.nombre and a.vinculacion = b.vinculacion)
'''
)



                      
#se cruza la info que se tiene que actualizar  a la info planta
dataupdate1=dim_planta_activa_nuevo_5.join(update_planta_activa_6,on=['id','valid_from','valid_to'],how='left')

# dataupdate1=dim_planta_activa_nuevo_5.join(update_planta_activa_6,(dim_planta_activa_nuevo_5.id==update_planta_activa_6.id2) & 
# (dim_planta_activa_nuevo_5.valid_from==update_planta_activa_6.valid_from2 ) & 
# (dim_planta_activa_nuevo_5.valid_to==update_planta_activa_6.valid_to2 ) ,how='left')

#se actualiza la informacion que corresponda
dataupdate1=dataupdate1.withColumn("terminacion",when((dataupdate1.max_terminacion.isNull()), lit(dataupdate1.terminacion)).otherwise(lit(dataupdate1.max_terminacion)))
dataupdate1=dataupdate1.withColumn("activo",when((dataupdate1.min_activo.isNull()), lit(dataupdate1.activo)).otherwise(lit(dataupdate1.min_activo)))


#se elimina columnas de ayuda para la actualizacion
dim_planta_activa_nuevo_6=dataupdate1.drop('max_terminacion','min_activo').persist(StorageLevel.DISK_ONLY_2)

#parte  7
dim_planta_activa_nuevo_7=dim_planta_activa_nuevo_6.withColumn("valid_to",
       when((col("terminacion").isNull()) | (col("terminacion") > dim_planta_activa_nuevo_6.valid_to),dim_planta_activa_nuevo_6.valid_to)
      .otherwise(dim_planta_activa_nuevo_6.terminacion)).persist(StorageLevel.DISK_ONLY_2)


dim_planta_activa_nuevo_7.createTempView('dim_planta_activa_nuevo_7')
# parte 8
update_planta_activa_8_1=spark.sql(f'''
select 
dim_planta_activa_nuevo_7.id as id,
dim_planta_activa_nuevo_7.direct_report_final,
planta_2.activo,
planta_3.nombre as planta_3_nombre ,
planta_4.nombre as planta_4_nombre,
planta_3.valid_from as planta_3_valid_from,
dim_planta_activa_nuevo_7.valid_from as valid_from,
dim_planta_activa_nuevo_7.valid_to as valid_to,
dim_planta_activa_nuevo_7.direct_report


from dim_planta_activa_nuevo_7
left join dim_planta_activa_nuevo_7 planta_2
on (replace(trim(dim_planta_activa_nuevo_7.direct_report),'  ',' ') = replace(trim(planta_2.nombre),'  ',' ')
and ((dim_planta_activa_nuevo_7.valid_from between planta_2.valid_from and planta_2.valid_to)
        or (dim_planta_activa_nuevo_7.valid_to between planta_2.valid_from and planta_2.valid_to))
and planta_2.dead = 0)
left join (select nombre, COUNT(*) contador, MAX(valid_from) valid_from from dim_planta_activa_nuevo_7 where dead = 0 and activo = 1 group by nombre) planta_3 on (replace(trim(dim_planta_activa_nuevo_7.direct_report),'  ',' ') = replace(trim(planta_3.nombre),'  ',' ') and (planta_3.valid_from between dim_planta_activa_nuevo_7.valid_from and dim_planta_activa_nuevo_7.valid_to))
left join (select nombre,vinculacion, COUNT(*) contador, min(valid_from) valid_from,MAX(valid_to) valid_to from dim_planta_activa_nuevo_7
 where activo = 0 group by nombre,vinculacion) planta_4 on (replace(trim(dim_planta_activa_nuevo_7.direct_report),'  ',' ') = replace(trim(planta_4.nombre),'  ',' ') and ((dim_planta_activa_nuevo_7.valid_from between planta_4.valid_from and planta_4.valid_to) or (dim_planta_activa_nuevo_7.valid_to between planta_4.valid_from and planta_4.valid_to)))


'''
)

#se actualiza la informacion que corresponda
update_planta_activa_8_2=update_planta_activa_8_1.withColumn("direct_report_final_calc",
       when(col("activo")==1,dim_planta_activa_nuevo_6.direct_report)
       .when((col("planta_3_nombre").isNotNull()) & (col("planta_3_valid_from").between(update_planta_activa_8_1.valid_from,update_planta_activa_8_1.valid_to)),update_planta_activa_8_1.planta_3_nombre)
       .when((col("planta_4_nombre").isNotNull()),update_planta_activa_8_1.direct_report)
      .otherwise('Inactive'))

update_planta_activa_8_2=update_planta_activa_8_2.select('id','valid_from','valid_to','direct_report_final_calc').distinct().persist(StorageLevel.DISK_ONLY_2)

  #join 3campos                    

#se cruza la info que se tiene que actualizar  a la info planta
update_planta_activa_8_3=dim_planta_activa_nuevo_7.join(update_planta_activa_8_2,on=["id","valid_from","valid_to"] ,how='left')

# update_planta_activa_8_3=dim_planta_activa_nuevo_7.join(update_planta_activa_8_2,(dim_planta_activa_nuevo_7.id==update_planta_activa_8_2.id_2) & 
# (dim_planta_activa_nuevo_7.valid_from==update_planta_activa_8_2.dim_planta_activa_nuevo_7_valid_from ) & 
# (dim_planta_activa_nuevo_7.valid_to==update_planta_activa_8_2.dim_planta_activa_nuevo_7_valid_to ) ,how='left')


dim_planta_activa_nuevo_8=update_planta_activa_8_3.withColumn("direct_report_final",
when(update_planta_activa_8_3.direct_report_final_calc.isNull(),update_planta_activa_8_3.direct_report_final).
otherwise(update_planta_activa_8_3.direct_report_final_calc)).drop('id_2','dim_planta_activa_nuevo_7_valid_to','dim_planta_activa_nuevo_7_valid_from').distinct().persist(StorageLevel.DISK_ONLY_2)
       

var_direct_report='Guzman Uribe Juan David'
#parte 9
dim_planta_activa_nuevo_9=dim_planta_activa_nuevo_8.withColumn('pais_2',when(dim_planta_activa_nuevo_8.id==8807,'Colombia').otherwise(dim_planta_activa_nuevo_8.pais_2))
#parte 10
dim_planta_activa_nuevo_10=dim_planta_activa_nuevo_9.withColumn('pais_2',when(dim_planta_activa_nuevo_9.pais_2.isNull(),dim_planta_activa_nuevo_9.pais).otherwise(dim_planta_activa_nuevo_9.pais_2))
#parte 11
dim_planta_activa_nuevo_11=dim_planta_activa_nuevo_10.withColumn('segmento',when( (dim_planta_activa_nuevo_10.direct_report==var_direct_report)& (col("segmento").isNull()),'SMB').otherwise(dim_planta_activa_nuevo_10.segmento) )
#parte 12
dim_planta_activa_nuevo_12=dim_planta_activa_nuevo_11.select(ordercolumns).withColumn('segmento',when( (dim_planta_activa_nuevo_11.direct_report!=var_direct_report)& (col("segmento").isNull()),'Mid Market').otherwise(dim_planta_activa_nuevo_11.segmento) )

#columnas para agregar por Nombre de empleado
df_fecha_nacimiento=nueva_planta.select("NOMBRE_EMPLEADO","FECHA_DE_NACIMIENTO").distinct().withColumnRenamed("FECHA_DE_NACIMIENTO","fecha_nacimiento")

dim_planta_activa_nuevo_13=dim_planta_activa_nuevo_12.join(df_fecha_nacimiento,dim_planta_activa_nuevo_12.nombre==df_fecha_nacimiento.NOMBRE_EMPLEADO,how="left")
dim_planta_activa_nuevo_13=dim_planta_activa_nuevo_13.select(ordercolumns)

dim_planta_activa_nuevo_12.write.option("tempdir",'s3n://'+bucket).option('header','true').mode('overwrite').option("partitionOverwriteMode", "dynamic").parquet('s3n://'+bucket+'/output/dim_planta_activa_temp.parquet')
dim_planta_activa_temp=spark.read.format("parquet").option("tempdir",'s3n://'+bucket).option('header','true').option("delimiter", "|").option("partitionOverwriteMode", "dynamic").load('s3n://'+bucket+'/output/dim_planta_activa_temp.parquet').persist(StorageLevel.DISK_ONLY_2)
dim_planta_activa_temp.write.option("tempdir",'s3n://'+bucket).option('header','true').mode('overwrite').option("partitionOverwriteMode", "dynamic").parquet('s3n://'+bucket+'/output/dim_planta_activa.parquet')