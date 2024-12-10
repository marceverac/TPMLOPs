import pandas as pd
import boto3

# Configurar cliente de S3
s3 = boto3.client('s3')

# Nombre del bucket y archivos
bucket_name = 'grupo-10'
input_file = 'productos_filtrados.csv'
output_file = 'topProduct.csv'

# Descargar el archivo desde S3
s3.download_file(bucket_name, input_file, f'/tmp/{input_file}')

# Leer el archivo descargado
df = pd.read_csv(f'/tmp/{input_file}')


#Realizamos los calculos
most_viewed_data = df.groupby(['advertiser_id', 'product_id', 'date']).size().reset_index(name='views')
most_viewed_sorted = most_viewed_data.sort_values(by=['advertiser_id', 'views'], ascending=[True, False])
top_20_most_viewed_per_advertiser = most_viewed_sorted.groupby('advertiser_id').head(20)

# Guardamos el csv en temporal
topProduct_path = f'/tmp/{output_file}'
top_20_most_viewed_per_advertiser.to_csv(topProduct_path, index=False)

# Subimos el archivo generado a S3
s3.upload_file(topProduct_path, bucket_name, output_file)