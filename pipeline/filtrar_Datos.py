import pandas as pd
from datetime import datetime
import boto3

s3 = boto3.client('s3')

bucket_name = 'grupo-10'
advertiser_file = 'advertiser_ids.csv'
ads_file = 'ads_views.csv'
productos_file = 'product_views.csv'

# Descargar los archivos CSV desde S3
s3.download_file(bucket_name, advertiser_file, f'/tmp/{advertiser_file}')
s3.download_file(bucket_name, ads_file, f'/tmp/{ads_file}')
s3.download_file(bucket_name, productos_file, f'/tmp/{productos_file}')

#Conseguimos la fecha actual para poder quedarnos solo con los datos de hoy
fecha_actual = datetime.now().strftime('%Y-%m-%d')

#Leemos csv
active_advertiser = pd.read_csv(f'/tmp/{advertiser_file}')
ads = pd.read_csv(f'/tmp/{ads_file}')
productos = pd.read_csv(f'/tmp/{productos_file}')

#Pasamos id a string por las dudas
active_advertiser['advertiser_id'] = active_advertiser['advertiser_id'].astype(str)
ads['advertiser_id'] = ads['advertiser_id'].astype(str)
productos['advertiser_id'] = productos['advertiser_id'].astype(str)

#Filtramos a los activos
ads_filtrados = ads[ads['advertiser_id'].isin(active_advertiser['advertiser_id'])]
productos_filtrados = productos[productos['advertiser_id'].isin(active_advertiser['advertiser_id'])]

#Filtramos por fecha
ads_filtrados = ads_filtrados[ads_filtrados['date'] == fecha_actual]
productos_filtrados = productos_filtrados[productos_filtrados['date'] == fecha_actual]

#Guardaremos los csv en temp para no ocupar espacio en el ec2
ads_filtrados_path = '/tmp/ads_filtrados.csv'
productos_filtrados_path = '/tmp/productos_filtrados.csv'

#Generamos csv
ads_filtrados.to_csv(ads_filtrados_path, index=False)
productos_filtrados.to_csv(productos_filtrados_path, index=False)

#Subimos los csv al S3
s3.upload_file(ads_filtrados_path, bucket_name, 'ads_filtrados.csv')
s3.upload_file(productos_filtrados_path, bucket_name, 'productos_filtrados.csv')
