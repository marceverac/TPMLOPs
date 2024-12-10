import pandas as pd
import boto3
import time


# Configurar cliente de S3
s3 = boto3.client('s3')
bucket_name = 'grupo-10'
input_file = 'ads_filtrados.csv'
output_file = 'topCTR.csv'

try:
    s3.download_file(bucket_name, input_file, f'/tmp/{input_file}')
    df = pd.read_csv(f'/tmp/{input_file}')
except Exception:
    time.sleep(300)
    s3.download_file(bucket_name, input_file, f'/tmp/{input_file}')
    df = pd.read_csv(f'/tmp/{input_file}')




# Descargar el archivo desde S3
s3.download_file(bucket_name, input_file, f'/tmp/{input_file}')

# Leer el archivo descargado
df = pd.read_csv(f'/tmp/{input_file}')

#Realizamos los calculos
df['click'] = (df['type'] == 'click').astype(int)
df['impression'] = (df['type'] == 'impression').astype(int)
ctr_data = df.groupby(['advertiser_id', 'product_id', 'date'], as_index=False).agg({'click': 'sum','impression': 'sum'})
ctr_data['CTR (%)'] = (ctr_data['click'] / ctr_data['impression']) * 100
ctr_data['CTR (%)'] = ctr_data['CTR (%)'].fillna(0)
ctr_data_sorted = ctr_data.sort_values(by=['advertiser_id', 'CTR (%)'], ascending=[True, False])
top_20_per_advertiser = ctr_data_sorted.groupby('advertiser_id').head(20)
top_20_per_advertiser.to_csv('topCTR.csv', index=False)

#Guardamos en el temporal
topCTR_path = f'/tmp/{output_file}'
top_20_per_advertiser.to_csv(topCTR_path, index=False)

# Subir el archivo generado a S3
s3.upload_file(topCTR_path, bucket_name, output_file)