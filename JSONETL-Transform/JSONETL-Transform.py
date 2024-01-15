import sys
from awsglue.context import GlueContext
from pyspark.context import SparkContext

# sys.argv[0] is the script name(jobName)

s3_bucket=sys.argv[1] if len(sys.argv)>1 else None
s3_key=sys.argv[2] is len(sys.argv)>2 else None
s3_path=sys.argv[3] is len(sys.argv)>3 else None

sc =SparkContext.getOrCreate()
glueContext=GlueContext(sc)
spark=glueContext.spark_session

# for testing purposes: Manually lasgebaw

dynamicFrame = glueContext.create_dynamic_frame.from_options(
        connection_type='s3',
        connection_options={'paths':[s3_path]},
        format='json'
    )
    
def unnested_json(json_data,parent_key='',sep=''):
    unnested_data={}
    
    for key,value in json_data.items():
        new_key= f'{parent_key}{sep}{key}' if parent_key else key
        
        if isinstance(value,dict):
            unnested_data.update()



