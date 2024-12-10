import psycopg2
import pandas as pd
import boto3
from datetime import date

# Configurar cliente de S3
s3 = boto3.client('s3')

# Parámetros de la base de datos PostgreSQL
db_params = {
    'host': 'grupo-10-rds2.cf4i6e6cwv74.us-east-1.rds.amazonaws.com',  # Endpoint del RDS
    'database': 'postgres',     
    'user': 'postgres',               
    'password': 'grupo10mlops',        
    'port': '5432',                    
}

# Nombre del bucket y archivos en S3
bucket_name = 'grupo-10'
topCTR_file = 'topCTR.csv'
topProduct_file = 'topProduct.csv'

# Descargar archivos desde S3 y guardarlos localmente
def download_csv_from_s3(bucket_name, file_name, local_path):
    s3.download_file(bucket_name, file_name, local_path)
    return local_path

# Validar columnas de los DataFrames
def validate_columns(dataframe, expected_columns):
    missing_columns = [col for col in expected_columns if col not in dataframe.columns]
    if missing_columns:
        raise ValueError(f"Faltan columnas en el DataFrame: {missing_columns}")

# Crear tablas y verificar columnas
def create_tables_if_not_exist():
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    # Crear tabla `top_ctr`
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS top_ctr (
            advertiser_id VARCHAR(255),
            product_id VARCHAR(255),
            click INT,
            impression INT,
            ctr FLOAT,
            processing_date VARCHAR(255)
        );
    """)

    # Verificar si falta la columna `processing_date` y agregarla si no existe
    cursor.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 
                FROM information_schema.columns 
                WHERE table_name='top_ctr' AND column_name='processing_date'
            ) THEN
                ALTER TABLE top_ctr ADD COLUMN processing_date VARCHAR(255);
            END IF;
        END $$;
    """)

    # Crear índice único para evitar duplicados
    cursor.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_top_ctr 
        ON top_ctr (advertiser_id, product_id, processing_date);
    """)

    # Crear tabla `top_products`
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS top_products (
            advertiser_id VARCHAR(255),
            product_id VARCHAR(255),
            views INT,
            processing_date VARCHAR(255)
        );
    """)

    # Verificar si falta la columna `processing_date` y agregarla si no existe
    cursor.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 
                FROM information_schema.columns 
                WHERE table_name='top_products' AND column_name='processing_date'
            ) THEN
                ALTER TABLE top_products ADD COLUMN processing_date VARCHAR(255);
            END IF;
        END $$;
    """)

    # Crear índice único para evitar duplicados
    cursor.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_top_products 
        ON top_products (advertiser_id, product_id, processing_date);
    """)

    conn.commit()
    cursor.close()
    conn.close()

# Manejo de valores 'inf'
def process_dataframe(dataframe):
    # Reemplazar valores 'inf' o '-inf' en CTR (%) por 0
    dataframe.replace([float('inf'), -float('inf')], 0, inplace=True)
    return dataframe

# Función para insertar datos en PostgreSQL
def insert_into_postgres(table_name, dataframe):
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    # Convertir los datos del dataframe a una lista de tuplas
    rows = [tuple(x) for x in dataframe.to_numpy()]
    columns = ', '.join(dataframe.columns)

    # Crear query de inserción
    placeholders = ', '.join(['%s'] * len(dataframe.columns))
    insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"

    # Ejecutar inserciones
    cursor.executemany(insert_query, rows)
    conn.commit()
    cursor.close()
    conn.close()

# Proceso principal
if __name__ == "__main__":

    topCTR_columns = ['advertiser_id', 'product_id', 'click', 'impression', 'CTR (%)']
    topProduct_columns = ['advertiser_id', 'product_id', 'views']

    # Rutas locales para guardar los CSV descargados
    local_topCTR_path = '/tmp/topCTR.csv'
    local_topProduct_path = '/tmp/topProduct.csv'

    # Descargar los archivos desde S3
    download_csv_from_s3(bucket_name, topCTR_file, local_topCTR_path)
    download_csv_from_s3(bucket_name, topProduct_file, local_topProduct_path)

    # Leer los archivos localmente
    topCTR_data = pd.read_csv(local_topCTR_path)
    topProduct_data = pd.read_csv(local_topProduct_path)

    # Validar columnas de los DataFrames
    validate_columns(topCTR_data, topCTR_columns)
    validate_columns(topProduct_data, topProduct_columns)

    # Renombrar columna `CTR (%)` a `ctr`
    topCTR_data.rename(columns={'CTR (%)': 'ctr'}, inplace=True)

    # Manejar valores 'inf'
    topCTR_data = process_dataframe(topCTR_data)
    topProduct_data = process_dataframe(topProduct_data)

    # Añadir la columna `processing_date` con la fecha actual como string
    today_date = date.today().strftime('%Y-%m-%d')  # Convertir a string en formato 'YYYY-MM-DD'
    topCTR_data['processing_date'] = today_date
    topProduct_data['processing_date'] = today_date

    # Crear tablas si no existen y verificar columnas
    create_tables_if_not_exist()

    # Insertar datos en PostgreSQL
    insert_into_postgres('top_ctr', topCTR_data)        # Tabla para CTR
    insert_into_postgres('top_products', topProduct_data)  # Tabla para Productos
