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

BUCKET_DATALAKE = 'ubits-datalake'
BUCKET_OPERATIONS = 'strategy-ops'
OUTPUT_PATH = 'DB/strategy-ops-google-sheets/tablas_dimensiones/output/engagement_analysis.parquet'

HUBSPOT_ENGAGEMENT_CALL = 'hubspot/hubspot_engagements_call.parquet/'
HUBSPOT_ENGAGEMENT_EMAIL = 'hubspot/hubspot_engagements_email.parquet/'
HUBSPOT_ENGAGEMENT_MEETING = 'hubspot/hubspot_engagements_meeting.parquet/'
HUBSPOT_ENGAGEMENT_NOTE = 'hubspot/hubspot_engagements_note.parquet/'
HUBSPOT_ENGAGEMENT_TASK = 'hubspot/hubspot_engagements_task.parquet/'
HUBSPOT_OWNERS = 'hubspot/hubspot_owners.parquet/'
HUBSPOT_DISPOSITIONS = 'hubspot/hubspot_dispositions.parquet/'
HUBSPOT_CONTACTS = 'hubspot/hubspot_contacts.parquet/'

DIM_DATES = 'DB/strategy-ops-google-sheets/tablas_dimensiones/dim_dates.parquet/'
DIM_DISPOSITIONS = 'DB/strategy-ops-google-sheets/tablas_dimensiones/dim_dispositions.parquet/'
DIM_COHORT_COMERCIAL = 'DB/strategy-ops-google-sheets/tablas_dimensiones/dim_cohort_comercial.parquet/'
PLANTA_VW = 'DB/strategy-ops-google-sheets/tablas_dimensiones/output/planta_view.parquet'

hubspot_engagement_call = spark.read.format("parquet").option("tempdir",'s3n://'+BUCKET_DATALAKE)\
                    .option('header','true').option("delimiter", "|").option("partitionOverwriteMode", "dynamic")\
                    .load("s3n://"+BUCKET_DATALAKE+"/"+HUBSPOT_ENGAGEMENT_CALL).persist(StorageLevel.DISK_ONLY_2)

hubspot_engagement_email  = spark.read.format("parquet").option("tempdir",'s3n://'+BUCKET_DATALAKE)\
                    .option('header','true').option("delimiter", "|").option("partitionOverwriteMode", "dynamic")\
                    .load("s3n://"+BUCKET_DATALAKE+"/"+HUBSPOT_ENGAGEMENT_EMAIL).persist(StorageLevel.DISK_ONLY_2)

hubspot_engagement_meeting = spark.read.format("parquet").option("tempdir",'s3n://'+BUCKET_DATALAKE)\
                    .option('header','true').option("delimiter", "|").option("partitionOverwriteMode", "dynamic")\
                    .load("s3n://"+BUCKET_DATALAKE+"/"+HUBSPOT_ENGAGEMENT_MEETING).persist(StorageLevel.DISK_ONLY_2)

hubspot_engagement_note = spark.read.format("parquet").option("tempdir",'s3n://'+BUCKET_DATALAKE)\
                    .option('header','true').option("delimiter", "|").option("partitionOverwriteMode", "dynamic")\
                    .load("s3n://"+BUCKET_DATALAKE+"/"+HUBSPOT_ENGAGEMENT_NOTE).persist(StorageLevel.DISK_ONLY_2)

hubspot_engagement_task = spark.read.format("parquet").option("tempdir",'s3n://'+BUCKET_DATALAKE)\
                    .option('header','true').option("delimiter", "|").option("partitionOverwriteMode", "dynamic")\
                    .load("s3n://"+BUCKET_DATALAKE+"/"+HUBSPOT_ENGAGEMENT_TASK).persist(StorageLevel.DISK_ONLY_2)

hubspot_owners = spark.read.format("parquet").option("tempdir",'s3n://'+BUCKET_DATALAKE)\
                    .option('header','true').option("delimiter", "|").option("partitionOverwriteMode", "dynamic")\
                    .load("s3n://"+BUCKET_DATALAKE+"/"+HUBSPOT_OWNERS).persist(StorageLevel.DISK_ONLY_2)

hubspot_dispositions = spark.read.format("parquet").option("tempdir",'s3n://'+BUCKET_DATALAKE)\
                    .option('header','true').option("delimiter", "|").option("partitionOverwriteMode", "dynamic")\
                    .load("s3n://"+BUCKET_DATALAKE+"/"+HUBSPOT_DISPOSITIONS).persist(StorageLevel.DISK_ONLY_2)

# hubspot_contacts = spark.read.format("parquet").option("tempdir",'s3n://'+BUCKET_DATALAKE)\
#                     .option('header','true').option("delimiter", "|").option("partitionOverwriteMode", "dynamic")\
#                     .load("s3n://"+BUCKET_DATALAKE+"/"+HUBSPOT_CONTACTS).persist(StorageLevel.DISK_ONLY_2)

dim_dates = spark.read.format("parquet").option("tempdir",'s3n://'+BUCKET_OPERATIONS)\
                    .option('header','true').option("delimiter", "|").option("partitionOverwriteMode", "dynamic")\
                    .load("s3n://"+BUCKET_OPERATIONS+"/"+DIM_DATES).persist(StorageLevel.DISK_ONLY_2)

dim_dispositions = spark.read.format("parquet").option("tempdir",'s3n://'+BUCKET_OPERATIONS)\
                    .option('header','true').option("delimiter", "|").option("partitionOverwriteMode", "dynamic")\
                    .load("s3n://"+BUCKET_OPERATIONS+"/"+DIM_DISPOSITIONS).persist(StorageLevel.DISK_ONLY_2)

dim_cohort_comercial = spark.read.format("parquet").option("tempdir",'s3n://'+BUCKET_OPERATIONS)\
                    .option('header','true').option("delimiter", "|").option("partitionOverwriteMode", "dynamic")\
                    .load("s3n://"+BUCKET_OPERATIONS+"/"+DIM_COHORT_COMERCIAL).persist(StorageLevel.DISK_ONLY_2)

planta_vw = spark.read.format("parquet").option("tempdir",'s3n://'+BUCKET_OPERATIONS)\
                    .option('header','true').option("delimiter", "|").option("partitionOverwriteMode", "dynamic")\
                    .load("s3n://"+BUCKET_OPERATIONS+"/"+PLANTA_VW).persist(StorageLevel.DISK_ONLY_2)
engagement = hubspot_engagement_call.unionByName(hubspot_engagement_meeting, allowMissingColumns=True)
engagement = engagement.unionByName(hubspot_engagement_note, allowMissingColumns=True)
engagement = engagement.unionByName(hubspot_engagement_task, allowMissingColumns=True)
engagement = engagement.unionByName(hubspot_engagement_email, allowMissingColumns=True).persist(StorageLevel.DISK_ONLY_2)

for col in engagement.columns:
    engagement = engagement.withColumnRenamed(col, col.replace('properties.',''))

### Cast Columns
engagement = engagement.withColumn('hs_timestamp', f.col('hs_timestamp').cast('date')).alias('engagement')

### Join Stamp Dataframes
stamp_dataframe = engagement.join(hubspot_dispositions.alias('dispositions'), engagement.hs_call_disposition==hubspot_dispositions.id, how='left')\
                    .join(hubspot_owners.alias('owners'), engagement.hubspot_owner_id==hubspot_owners.id, 'left')\
                    .join(dim_dates.alias('stamp_date'), engagement.hs_timestamp==dim_dates.dte, 'left')\
                    .join(planta_vw.alias('planta'), (engagement.hubspot_owner_id==planta_vw.owner_id)&(planta_vw.valid_from<=dim_dates.dte)&(dim_dates.dte<=planta_vw.valid_to), 'left')\
                    .persist(StorageLevel.DISK_ONLY_2)

stamp_dataframe = stamp_dataframe.join(dim_cohort_comercial.alias('stamp_cohort'), (dim_cohort_comercial.StartDate<=stamp_dataframe.ingreso)&(stamp_dataframe.ingreso<=dim_cohort_comercial.EndDate), 'left')
### Select and Rename Columns                    
stamp_dataframe = stamp_dataframe.select('engagement.id',
                            'stamp_date.yyyy', 'stamp_date.mm', 'stamp_date.ww', 'stamp_date.dte',
                            'planta.ingreso','planta.subteam','planta.subteam_active',
                            f.col('dispositions.label').alias('call_outcome'),
                            f.col('engagement.hubspot_owner_id').alias('ownerId'),
                            f.col('engagement.hs_activity_type').alias('type'),
                            f.col('engagement.hs_num_associated_companies').alias('companyId'),
                            f.col('engagement.hs_num_associated_contacts').alias('contactIds'),
                            f.col('engagement.hs_timestamp').alias('timestamp'),
                            f.col('engagement.hs_call_disposition').alias('disposition'),
                            f.col('engagement.hubspot_owner_id').alias('ownerId'),
                            f.col('engagement.createdAt').cast('date'),
                            f.col('engagement.hs_meeting_outcome').alias('meeting_outcome'),
                            f.col('engagement.hs_activity_type_status').alias('engagement_status'),
                            f.col('engagement.hs_call_duration').alias('call_duration'),
                            f.col('stamp_date.week_start').alias('timestamp_week_start'),
                            f.col('planta.nombre').alias('bd'),
                            f.col('planta.area').alias('team'),
                            f.col('planta.pais_2').alias('pais'),                         
                            f.col('stamp_cohort.Name').alias('stamp_cohort'))

### Create Temporal Columns
stamp_dataframe = stamp_dataframe.withColumn('temp_days_since_entry', f.datediff(f.col('dte'),f.col('ingreso'))+1)\
                    .withColumn('temp_datepart_ingreso', f.dayofmonth(f.col('ingreso')))\
                    .withColumn('temp_months_between', f.round(f.months_between(f.col('dte'),f.col('ingreso'),True),0))\
                    .withColumn('temp_week_between',(f.col('temp_days_since_entry')/7)+1)\
                    .withColumn('temp_diff_week_between', f.datediff(f.date_add(f.col('dte'),-1), f.date_add(f.col('ingreso'),-1))+1)\
                    .persist(StorageLevel.DISK_ONLY_2)

### Create Columns                         
stamp_dataframe = stamp_dataframe.withColumn('stamp_cohort', f.when(f.col('stamp_cohort').isNull(), 'Unidentified')\
                        .otherwise(f.col('stamp_cohort')))\
                        .withColumn('days_since_entry', f.col('temp_days_since_entry'))\
                        .withColumn('weeks_since_entry', f.when((f.col('type')=='MEETING')&(f.col('temp_week_between')>=1)&(f.col('temp_week_between')<=2), 2)\
                            .when(f.col('temp_diff_week_between')<=12, f.col('temp_diff_week_between'))\
                            .otherwise(f.lit(13)))\
                        .withColumn('months_since_entry', f.when(f.col('temp_datepart_ingreso')<=15, f.col('temp_months_between')+1)\
                            .otherwise(f.col('temp_months_between')+0))\
                        .withColumn('months_since_entry', f.when(f.col('months_since_entry')<=7, f.col('months_since_entry'))\
                            .otherwise(f.lit(7)))\
                        .withColumn('actual_weeks_since_entry', f.when((f.col('type')=='MEETING')&(f.col('temp_week_between')>=1)&(f.col('temp_week_between')<=2), 2)\
                            .otherwise(f.col('temp_diff_week_between')))\
                        .withColumn('actual_months_since_entry',f.when(f.col('temp_datepart_ingreso')<=15, f.col('temp_months_between')+1)\
                            .otherwise(f.col('temp_months_between')+0))\
                        .persist(StorageLevel.DISK_ONLY_2)

stamp_dataframe = stamp_dataframe.select('id', 'companyId', 'contactIds', 'type',
                        #'activity_type',
                        'yyyy', 'mm',
                        'ww', 'timestamp_week_start', 'dte', 'ingreso', 'days_since_entry', 'weeks_since_entry',
                        'months_since_entry','actual_weeks_since_entry', 'actual_months_since_entry', 'stamp_cohort',
                        'bd', 'team', 'subteam', 'subteam_active', 'pais', 'timestamp',
                        'call_outcome', 'meeting_outcome', 'engagement_status', 'call_duration',
                        'createdAt')\
                    .persist(StorageLevel.DISK_ONLY_2)

stamp_dataframe.show(5)
creation_dataframe = engagement.join(dim_dates.alias('creation_date'), engagement.hs_timestamp==dim_dates.dte, 'left')\
                    .join(planta_vw.alias('planta_creation'), (engagement.hubspot_owner_id==planta_vw.owner_id)&(planta_vw.valid_from<=dim_dates.dte)&(dim_dates.dte<=planta_vw.valid_to), 'left')\
                        .persist(StorageLevel.DISK_ONLY_2)
creation_dataframe = creation_dataframe.join(dim_cohort_comercial.alias('cohort_comercial'), (dim_cohort_comercial.StartDate<=creation_dataframe.ingreso)&(creation_dataframe.ingreso<=dim_cohort_comercial.EndDate), 'left')\
                        .persist(StorageLevel.DISK_ONLY_2)

creation_dataframe = creation_dataframe.select('engagement.id',
                            f.col('engagement.hs_activity_type').alias('type'),
                            f.col('creation_date.yyyy').alias('creation_yyyy'),
                            f.col('creation_date.mm').alias('creation_mm'),
                            f.col('creation_date.ww').alias('creation_ww'),
                            f.col('creation_date.dte').alias('creation_dte'),
                            f.col('creation_date.week_start').alias('creation_week_start'),
                            f.col('planta_creation.ingreso').alias('ingreso_creacion'),
                            f.col('planta_creation.nombre').alias('creation_bd'),
                            f.col('planta_creation.area').alias('creation_team'),
                            f.col('planta_creation.subteam').alias('creation_subteam'),
                            f.col('planta_creation.subteam_active').alias('creation_subteam_active'),
                            f.col('planta_creation.pais_2').alias('creation_pais'),                            
                            f.col('cohort_comercial.Name').alias('creation_cohort'))\
                            .persist(StorageLevel.DISK_ONLY_2)

### Create Temporal Columns
creation_dataframe = creation_dataframe.withColumn('temp_days_since_entry', f.datediff(f.col('creation_dte'),f.col('ingreso_creacion'))+1)\
                    .withColumn('temp_datepart_ingreso', f.dayofmonth(f.col('ingreso_creacion')))\
                    .withColumn('temp_months_between', f.round(f.months_between(f.col('creation_dte'),f.col('ingreso_creacion'),True),0))\
                    .withColumn('temp_week_between',(f.col('temp_days_since_entry')/7)+1)\
                    .withColumn('temp_diff_week_between', f.datediff(f.date_add(f.col('creation_dte'),-1), f.date_add(f.col('ingreso_creacion'),-1))+1)\
                    .persist(StorageLevel.DISK_ONLY_2)

### Create Columns                         
creation_dataframe = creation_dataframe.withColumn('creation_cohort', f.when(f.col('creation_cohort').isNull(), 'Unidentified')\
                        .otherwise(f.col('creation_cohort')))\
                        .withColumn('creation_days_since_entry', f.col('temp_days_since_entry'))\
                        .withColumn('creation_weeks_since_entry', f.when((f.col('type')=='MEETING')&(f.col('temp_week_between')>=1)&(f.col('temp_week_between')<=2), 2)\
                            .when(f.col('temp_diff_week_between')<=12, f.col('temp_diff_week_between'))\
                            .otherwise(f.lit(13)))\
                        .withColumn('creation_months_since_entry', f.when(f.col('temp_datepart_ingreso')<=15, f.col('temp_months_between')+1)\
                            .otherwise(f.col('temp_months_between')+0))\
                        .withColumn('creation_months_since_entry', f.when(f.col('creation_months_since_entry')<=7, f.col('creation_months_since_entry'))\
                            .otherwise(f.lit(7)))\
                        .withColumn('creation_actual_weeks_since_entry', f.when((f.col('type')=='MEETING')&(f.col('temp_week_between')>=1)&(f.col('temp_week_between')<=2), 2)\
                            .otherwise(f.col('temp_diff_week_between')))\
                        .withColumn('creation_actual_months_since_entry',f.when(f.col('temp_datepart_ingreso')<=15, f.col('temp_months_between')+1)\
                            .otherwise(f.col('temp_months_between')+0))\
                        .persist(StorageLevel.DISK_ONLY_2)

creation_dataframe = creation_dataframe.select('id', 'type', 'creation_yyyy', 'creation_mm',  'creation_ww', 'creation_week_start', 'creation_dte',
                        'ingreso_creacion', 'creation_days_since_entry', 'creation_weeks_since_entry', 'creation_months_since_entry','creation_actual_weeks_since_entry',
                        'creation_actual_months_since_entry', 'creation_cohort', 'creation_bd', 'creation_team', 'creation_subteam', 'creation_subteam_active', 'creation_pais')\
                    .persist(StorageLevel.DISK_ONLY_2)

creation_dataframe.show(5)
engagement_analysis = stamp_dataframe.join(creation_dataframe, stamp_dataframe.id==creation_dataframe.id, 'left')
engagement_analysis.show(5)
engagement_analysis.write.option("tempdir",'s3n://'+BUCKET_OPERATIONS).option('header','true')\
    .mode('overwrite').option("partitionOverwriteMode", "dynamic")\
    .parquet('s3n://'+BUCKET_OPERATIONS+'/'+OUTPUT_PATH)
