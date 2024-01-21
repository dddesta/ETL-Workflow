import sys
import boto3
import awswrangler as wr
import pandas as pd


# sys.argv[0] is the script name(jobName)

#s3_bucket=sys.argv[1] if len(sys.argv)>1 else None
#s3_key=sys.argv[2] if len(sys.argv)>2 else None
#s3_path=sys.argv[2] if len(sys.argv)>2 else None

#for testing purposes


input_bucket= 's3-etlproject-drop'
input_key= 'glanbia_test.json'
input_path='s3://s3-etlproject-drop/glanbia_test.json'

output_bucket= 'etl-final-destination'
output_key= 'processed_glanbia_test.parquet'
s3=boto3.client('s3')

def flatten_json(y):
    out = {}
 
    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + str(a) + '_')
        elif type(x) is list:
            for i, a in enumerate(x):
                flatten(a, name + str(i) + '_')
        else:
            # Convert non-string values to strings before concatenation
            out[name[:-1]] = str(x)
 
    flatten(y)
    return out
    
def failure_notification(bucketname,key,e):
    sns=boto3.client('sns')
    
    response=sns.publish(
        TopicArn='arn:aws:sns:us-east-1:385363378908:ETLWorkFlowTopic',
        Message= f'Json flattening failure: The file in {key} in {bucketname} has not been processed!\n \n Exception message: {e}',
        Subject='Flattening WorkFlow Update!'
        )
        
def success_notification(bucketname,key):
    sns=boto3.client('sns')
    
    response=sns.publish(
        TopicArn='arn:aws:sns:us-east-1:385363378908:ETLWorkFlowTopic',
        Message= f'Json flattening SUCCESS: The file in {key} in {bucketname} has been processed successfully!!',
        Subject='Flattening WorkFlow Update!'
        )
def s3_parquet_write(data,out_bucket,out_key):

    path=f's3://{out_bucket}/{out_key}'
    wr.s3.to_parquet(data, path)

def main_func():
    
    try:
        #read the json
        df=wr.s3.read_json(path=input_path)
        
        json_dict= df.to_dict(orient='records')
        
        result=flatten_json(json_dict)
        
        #print(f"Unnested Result Dict: {result}")
        
        output_df = pd.DataFrame([result])
        
        s3_parquet_write(output_df, output_bucket,output_key)
        
        success_notification(input_bucket,input_key)
        
    except Exception as e:
        failure_notification(input_bucket,input_key,e)

    print('Done!:)')
    
def lambda_handler(event, context):

    main_func()
    
