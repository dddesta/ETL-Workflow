import sys
import boto3
import awswrangler as wr

# sys.argv[0] is the script name(jobName)

s3_bucket=sys.argv[1] if len(sys.argv)>1 else None
s3_key=sys.argv[2] if len(sys.argv)>2 else None
s3_path=sys.argv[3] if len(sys.argv)>3 else None


# for testing purposes: Manually lasgebaw

df=wr.s3.read_json(path=s3_path,dataset=True)

    
def unnest_json(json_data,parent_key='',sep='_'):
    unnested_data={}
    
    for key,value in json_data.items():
        new_key= f'{parent_key}{sep}{key}' if parent_key else key

        #if the value is a dictionary
        if isinstance(value,dict):
            unnested_data.update(unnest_json(value,new_key,sep=sep))
        
        elif isinstance(value,list):
            for i,item in enumerate(value):
                unnested_data.update(unnest_json({str(i):item},new_key,sep=sep))
        #leaf value
        else:
            unnested_data[new_key]=value
    
    return unnested_data


result_dict=unnest_json(df)

destination_bucket = 'etl-final-destination'
destination_key = 'final.snappy.parquet'
dest_path=f's3://{destination_bucket}/{destination_key}'

wr.s3.to_parquet(df=result_df,path=dest_path,dataset=True, compression="SNAPPY")


