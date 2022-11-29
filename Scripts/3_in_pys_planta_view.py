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
dim_planta_activa=spark.read.format("parquet").option("tempdir",'s3n://'+bucket).option('header','true').option("delimiter", "|").option("partitionOverwriteMode", "dynamic").load('s3n://'+bucket+'/output/dim_planta_activa.parquet').persist(StorageLevel.DISK_ONLY_2)

orderColumnsOutput=["id",
"rank_id",
"reverse_rank_id",
"nombre",
"cargo",
"gente_a_cargo",
"area",
"cco",
"direct_report",
"pais",
"ingreso",
"nombre_hs",
"owner_id",
"hs_user_id",
"terminacion",
"dead",
"activo",
"valid_from",
"valid_from_inicio_mes",
"valid_to",
"subteam",
"subteam_active",
"cargo_type",
"rama_formacion",
"fecha_nacimiento",
"edad",
"genero",
"pais_2",
"segmento",
"ingreso_nuevo_cargo",
"flag_first_cargo"]

#string to date
dim_planta_activa=dim_planta_activa.withColumn("valid_from",to_date(col("valid_from"),"yyyy-mm-dd"))
dim_planta_activa=dim_planta_activa.withColumn("valid_to",to_date(col("valid_to"),"yyyy-mm-dd"))

#create row_number / activo
dim_planta_activa_view=dim_planta_activa.withColumn("rank_id",row_number().over(Window.partitionBy("nombre").orderBy(col('valid_from').asc()))).\
withColumn("reverse_rank_id",row_number().over(Window.partitionBy("nombre").orderBy(col('valid_from').desc()))).\
withColumn("activo", min("activo").over(Window.partitionBy("nombre","vinculacion")) )

#Add date+1 if rank_id=!1
dim_planta_activa_view = dim_planta_activa_view.withColumn("valid_from", when(dim_planta_activa_view.rank_id == 1,dim_planta_activa_view.valid_from).otherwise(date_add(dim_planta_activa_view.valid_from, 1)))

#primer dia del mes if rank_id=1 sino sumele 1 a la fecha
dim_planta_activa_view=dim_planta_activa_view.withColumn("valid_from_inicio_mes", when(dim_planta_activa_view.rank_id == 1,f.trunc("valid_from", "month")).otherwise(date_add(dim_planta_activa_view.valid_from, 1)))


#no hay data para validar
#Colocar en mayuscula las primeras palabras
dim_planta_activa_view=dim_planta_activa_view.withColumn("subteam",when( (dim_planta_activa_view.cargo.like("%Leader%"))  | (dim_planta_activa_view.cargo.like("%Head%") )| (dim_planta_activa_view.cargo.like("%Manager%") & ~dim_planta_activa_view.cargo.like("%key%")),dim_planta_activa_view.nombre).\
    otherwise(dim_planta_activa_view.direct_report))



dim_planta_activa_view=dim_planta_activa_view.withColumn("subteam_active",when( (dim_planta_activa_view.cargo.like("%Leader%"))  | (dim_planta_activa_view.cargo.like("%Head%") )\
| (dim_planta_activa_view.cargo.like("%Manager%") & ~dim_planta_activa_view.cargo.like("%key%"))\
& (dim_planta_activa_view.activo==0),"Inactive" ).\

when( (dim_planta_activa_view.cargo.like("%Leader%"))  | (dim_planta_activa_view.cargo.like("%Head%") )\
| (dim_planta_activa_view.cargo.like("%Manager%") & ~dim_planta_activa_view.cargo.like("%key%"))\
& (dim_planta_activa_view.activo!=0),dim_planta_activa_view.nombre ).otherwise(dim_planta_activa_view.direct_report_final)
 )

#se puso en mayuscula Customer y no customer
#se puso EXPANSIONS Y no Expansions
dim_planta_activa_view=dim_planta_activa_view.withColumn("cargo_type",when( (dim_planta_activa_view.cargo.like("%Chief Revenue Officer%")) & ( dim_planta_activa.area.like("%Sales%")),"CSO").\
when( (dim_planta_activa_view.cargo.like("%VP Sales%")) & ( dim_planta_activa.area.like("%Sales%")),"Sales VP").\
when( (dim_planta_activa_view.cargo.like("%Sales%Manager%")) & ( dim_planta_activa.area.like("%Sales%")),"Sales Manager").\
when( (lower(dim_planta_activa_view.cargo).like("%expansions%")) & ( dim_planta_activa.cargo.like("%Head%")),"Expansions Head").\
when( lower(dim_planta_activa_view.cargo).like("%expansions%"),"Expansions Expert").\
when( (dim_planta_activa_view.cargo.like("%Leader%") |  dim_planta_activa.area.like("%Head%"))  &  (dim_planta_activa_view.cargo.like("%Senior%") &  dim_planta_activa.area.like("%Sales%") & ~dim_planta_activa.cargo.like("%SDR%"))  ,"Sr Industry Head").\
when( (lower(dim_planta_activa_view.cargo).like("%Leader%") | dim_planta_activa.cargo.like("%Head%")) & dim_planta_activa_view.area.like("%Sales%")\
& ~dim_planta_activa_view.cargo.like("%SDR%") & ~lower(dim_planta_activa_view.cargo).like("%expansions%"),"Industry Head").\
when(dim_planta_activa.cargo.like('%Representative%')\
& (dim_planta_activa_view.area.like("%Sales%") | dim_planta_activa_view.area.like("%SDR%")) & (dim_planta_activa.valid_to>="2021-03-01"),"Sales Developer Representative").\
when(dim_planta_activa.cargo.like('%Representative%')\
& (dim_planta_activa_view.area.like("%Sales%") | dim_planta_activa_view.area.like("%SDR%")) & (dim_planta_activa.cargo.like("%Lead%")),"Sales Developer Representative Lead").\
when( (dim_planta_activa_view.cargo.like("%Representative%")) & ( dim_planta_activa.area.like("%growth%")),"Sales Developer Representative").\
when( (lower(dim_planta_activa_view.cargo).like("%SDR%")) & ( dim_planta_activa.area.like("%Head%")),"SDR Head").\
when( (dim_planta_activa_view.cargo.like("%Senior%")) & ( dim_planta_activa.area.like("%Sales%")),"Sr Business Developer").\
when( (dim_planta_activa_view.cargo.like("%Junior%")) & ( dim_planta_activa.area.like("%Sales%")),"Jr Business Developer").\
when( (dim_planta_activa_view.cargo.like("%Intern%")) & ( dim_planta_activa.area.like("%Sales%")),"Intern Comercial").\
when (dim_planta_activa_view.area.like("%Sales%"),"Business Developer").\
when( (dim_planta_activa_view.area.like("%Customer%")) & ( dim_planta_activa.cargo.like("%VP%")),"Customer Success VP").\
when( (dim_planta_activa_view.area.like("%Customer%")) & ( dim_planta_activa.cargo.isin("Customer Success Senior Manager")),"Customer Success Sr Manager").\
when( (dim_planta_activa_view.area.like("%Customer%")) & ( dim_planta_activa.cargo.isin("Customer Experience Manager")),"Customer Experience Manager").\
when( (dim_planta_activa_view.area.like("%Customer%")) & ( dim_planta_activa.cargo.isin('Customer Success Manager','Customer Sucess Manager')),"Customer Success Manager").\
when( (dim_planta_activa_view.area.like("%Customer%")) & ( dim_planta_activa.cargo.like('%Head%') & (upper(dim_planta_activa.cargo).like('%KAM%')  | upper(dim_planta_activa.cargo).like('%ACCOUNT%')) ),"KAMs Head").\
when( (dim_planta_activa_view.area.like("%Customer%")) & ( dim_planta_activa.cargo.like('%Head%') & upper(dim_planta_activa.cargo).like('%EXPANSION%') ),"Expansions Head").\
when( (dim_planta_activa_view.area.like("%Customer%")) & ( dim_planta_activa.cargo.like('%Head%')) ,"Customer Success Head").\

when( (dim_planta_activa_view.area.like("%Customer%")) & ( upper(dim_planta_activa.cargo).like('%SR%') | upper(dim_planta_activa.cargo).like('%SENIOR%') ) & (upper(dim_planta_activa.cargo).like('%KAM%')  | upper(dim_planta_activa.cargo).like('%ACCOUNT%') | upper(dim_planta_activa.cargo).like('%KEY%') ),"Sr KAM").\

when( dim_planta_activa_view.area.like("%Customer%") &  (upper(dim_planta_activa.cargo).like('%KAM%') | upper(dim_planta_activa.cargo).like('%ACCOUNT%') | upper(dim_planta_activa.cargo).like('%KEY%')) ,"KAM").\
when( (dim_planta_activa_view.area.like("%Customer%")) & ( upper(dim_planta_activa.cargo).like('%EXPANSIONS%')) ,"Expansions Expert").otherwise("Other"))

dim_planta_activa_view = dim_planta_activa_view.withColumn("edad", f.floor(f.datediff(f.current_date(), f.col("fecha_nacimiento"))/365.25))
dim_planta_activa_view=dim_planta_activa_view.withColumn("ingreso_nuevo_cargo", min(col("valid_from")).over(Window.partitionBy([dim_planta_activa_view.nombre,dim_planta_activa_view.cargo_type])) )
dim_planta_activa_view=dim_planta_activa_view.withColumn("flag_first_cargo",when(dim_planta_activa_view.ingreso_nuevo_cargo==dim_planta_activa_view.ingreso,1).otherwise(0))

dim_planta_activa_view=dim_planta_activa_view.select(orderColumnsOutput)

#write
dim_planta_activa_view.write.option("tempdir",'s3n://'+bucket).option('header','true').mode('overwrite').option("partitionOverwriteMode", "dynamic").parquet('s3n://'+bucket+'/output/planta_view.parquet')