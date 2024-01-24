import boto3
import pandas as pd
import awswrangler as wr

def

input_bucket_name = 's3-etlproject-drop'
input_object_key = 'data.csv'
output_bucket_name = 'etl-final-destination'
output_object_key = 'outputdata.parquet'

data_dictionary = {
    'SITE_CM360': 'string',
    'SITE_ID': 'string',
    'SITE_ID_SITE_DIRECTORY': 'int',
    'SITE_SITE_DIRECTORY': 'string',
    'FILENAME': 'string',
    'FILE_ROW_NUMBER': 'int',
    'INSERT_DATETIME': 'timestamp',
    'PARTITION_DATE': 'date'
}

#reading the csv and putting it on the pandas df
s3 = boto3.client('s3')

response = s3.get_object(Bucket=input_bucket_name, Key=input_object_key)
csv_content = response['Body'].read().decode('utf-8')

df = pd.read_csv(StringIO(csv_content))
df['INSERT_DATETIME'] = pd.to_datetime(df['INSERT_DATETIME'])

df['SITE_ID'] = df['SITE_ID'].astype(str)
df['PARTITION_DATE'] = pd.to_datetime(df['PARTITION_DATE']).dt.date

#Then translate the data dict into appropriate pyarrow names
# Convert the DataFrame to a PyArrow Table with enforced data types
table_schema = pa.schema([(col, pa.int64()) if data_type == 'int' else
                          (col, pa.timestamp('us')) if data_type == 'timestamp' else
                          (col, pa.date32()) if data_type == 'date' else
                          (col, pa.string())
                          for col, data_type in data_dictionary.items()])

#Then convert the DataFrame into a pyarrowtable
table = pa.Table.from_pandas(df, schema=table_schema)


# Write the PyArrow Table to a BytesIO object with Snappy compression
buffer = BytesIO()
pq.write_table(table, buffer, compression='snappy')

# Specify the destination S3 bucket and key for the Parquet file
destination_bucket = 'etl-final-destination'
destination_key = 'final.snappy.parquet'


# Upload the BytesIO buffer to S3
s3.put_object(Body=buffer.getvalue(), Bucket=output_bucket_name, Key=output_object_key)

