import skynetmodule as skm
import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials



def cleanData(df):
    df=df.fillna('')
    df = df.applymap(lambda s: s.lower() if type(s) == str else s) #data in minusc
    df1 = df.applymap(lambda s: int(s)  if s.isnumeric()==True   else s) #convert string to int
    print("entró")

    return df

def fnCheckDataS3(BUCKET, FILE_PATH,env): 
  #  bucket, 
   # filepath 
    vrKeyId,vrKeyAccess = skm.get_default_credentials("prod") 
   # bucket = 'analytics-ubits-production-dev' 
    #filepath = 'zoho/hubspot/engagement.parquet' 
    try: 
        df = skm.pd_read_s3_multiple_parquets(aws_id=vrKeyId,aws_secret=vrKeyAccess,filepath=FILE_PATH,bucket=BUCKET) 
        vrProcess = True   
    except : 
        vrProcess = False  
        df = [] 
    print(vrProcess)
    return vrProcess, df
    
def drive_to_s3_UsersProgress(env,bucket,path,nameSheet,id_sheets):
    df = skm.pd_read_gsheet(id_sheets,nameSheet)  # se debe de cambiar el id del excel por la que debe de ser
    #df=cleanData(df)
    vrAwsId, vrAwsSecret = skm.get_default_credentials(env) 
    df.name = nameSheet
    skm.pd_write_parquet_s3(df,bucket,path,vrAwsId,vrAwsSecret)
    return df



if __name__ == "__main__":
    id_sheets='1ZqGEr34CVO_PMIr5tQeoEPklhIrzs_OloacdK7pNqXo'
    listNameSheets=skm.get_sheets_names(id_sheets)
    ENV= 'PROD'
    BUCKET = 'strategy-ops' 
    PATH_DIM_PLANTA ='DB/strategy-ops-google-sheets/tablas_dimensiones/output/dim_planta_activa.parquet/'
                        
    lista=[]
    vrProcess,df=fnCheckDataS3(BUCKET,PATH_DIM_PLANTA,ENV)
    if vrProcess==True:
        print("No es la primera ejecucion")
        for  name in  range(len(listNameSheets)):
            nameParquet=listNameSheets[name]
            if nameParquet not in (['dim_planta_activa','deals_propertyhistory']): # deals_propertyhistory me falla toca solucionarlo
                print(nameParquet)

                PATH = f'DB/strategy-ops-google-sheets/tablas_dimensiones/{nameParquet}.parquet/{nameParquet}.parquet'
                df=drive_to_s3_UsersProgress(ENV,BUCKET,PATH,nameParquet,id_sheets)

                lista.append(df)
                print("insertó", nameParquet)
    else:
        for  name in  range(len(listNameSheets)):
            nameParquet=listNameSheets[name]
            if nameParquet not in (['deals_propertyhistory']): # deals_propertyhistory me falla toca solucionarlo
                print("Es la primera ejecucion")
                nameParquet=listNameSheets[name]


                PATH = f'DB/strategy-ops-google-sheets/tablas_dimensiones/{nameParquet}.parquet/{nameParquet}.parquet'
                df=drive_to_s3_UsersProgress(ENV,BUCKET,PATH,nameParquet,id_sheets)

                lista.append(df)
                print("insertó", nameParquet)
