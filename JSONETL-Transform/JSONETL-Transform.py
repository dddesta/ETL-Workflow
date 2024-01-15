import sys
from awsglue.context import GlueContext
from pyspark.context import SparkContext

# sys.argv[0] is the script name(jobName)

s3_bucket=sys.argv[1] if len(sys.argv)>1 else None
s3_key=sys.argv[2] if len(sys.argv)>2 else None
s3_path=sys.argv[3] if len(sys.argv)>3 else None

sc =SparkContext.getOrCreate()
glueContext=GlueContext(sc)
spark=glueContext.spark_session

# for testing purposes: Manually lasgebaw

dynamicFrame = glueContext.create_dynamic_frame.from_options(
        connection_type='s3',
        connection_options={'paths':[s3_path]},
        format='json'
    )
    
def unnest_json(json_data,parent_key='',sep='_'):
    unnested_data={}
    
    for key,value in json_data.items():
        new_key= f'{parent_key}{sep}{key}' if parent_key else key

        #if the value is a dictionary
        if isinstance(value,dict):
            unnested_data.update(unnest_json(value,new_key,sep=sep))
        
        elif isinstance(value,list):
            for i,item in enumerate(value):
                unnested_data.update(unnest_json({str(i)},new_key,sep=sep))
        #leaf value
        else:
            unnested_data[new_key]=value
    
    return unnested_data

reslut_df=unnest_json(dynamicFrame)

glueContext.write_dynamic_fram.from_options(
    frame=dynamicFrame,
    connection_type='s3',
    connection_options={
       s3://etl-final-destination/JSON Final/
    },
    format='parquet',
    format_options={'snappy'}
    )


