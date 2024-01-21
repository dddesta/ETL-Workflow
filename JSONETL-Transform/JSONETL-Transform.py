import sys
import boto3
import awswrangler as wr
import pandas as pd
from awsglue.utils import getResolvedoptions


s3=boto3.client('s3')
args = getResolvedoptions(sys.argv, ['input_bucket', 'input_key' ])


input_bucket=args['input_bucket']
input_key=args['input_key']
input_path=f's3://{input_bucket}/{input_key}'

output_bucket= 'etl-final-destination'
output_key= 'processed_glanbia_test.parquet'



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
        
        #change the df to a dictionary
        json_dict= df.to_dict(orient='records')
        
        #use the func flatten_json to unnest the json dictionary
        result=flatten_json(json_dict)
        
        #print(f"Unnested Result Dict: {result}")
        
        #change the output dict to a df to write as a parquet
        output_df = pd.DataFrame([result])
        
        if status == True:
            s3_parquet_write(df, output_bucket, output_key)
        
        success_notification(input_bucket,input_key)
        
    except Exception as e:
        failure_notification(input_bucket,input_key,e)

    print('Done!:)')
    


main_func()
    
