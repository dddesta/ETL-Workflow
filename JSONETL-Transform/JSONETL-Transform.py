import sys
import time
import json
import boto3
import pandas as pd
import awswrangler as wr
from awsglue.utils import getResolvedOptions

#func to start crawler
def trigger_crawler():
    try:
        glue=boto3.client('glue')
        glue.start_crawler('jsoncrawler')
    except Exception as e:
        print(e)
        print('Error starting crawler')
    
#func to flatten nested dicts 
def flatten_json(nested_json, exclude=[]):
    out = {}
 
    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                if a not in exclude: flatten(x[a], name + str(a) + '_')
        #not needed if not flattening lists
        elif type(x) is list:
            for i, a in enumerate(x):
                flatten(a, name + str(i) + '_')
        else:
            # Convert non-string values to strings before concatenation
            out[name[:-1]] = str(x)
 
    flatten(nested_json)
    return out
  
#func to send sns notifications to an sns note topic. success and failure  
def sns_notification(bucketname,key,status=False,e=''):
    sns=boto3.client('sns')
    sub='Flattening WorkFlow Update!'
    
    if status:
        mssg=f'Json flattening SUCCESS: The file in {key} in {bucketname} has been processed successfully!!'
    else:
        mssg=f'Json flattening failure: The file in {key} in {bucketname} has not been processed!\n \n Exception message: {e}'

    response=sns.publish(
        TopicArn='arn:aws:sns:us-east-1:385363378908:ETLWorkFlowTopic',
        Message= mssg,
        Subject= sub
        )
            
def s3_parquet_write(data,out_bucket,out_key):
    path=f's3://{out_bucket}/{out_key}'
    wr.s3.to_parquet(data, path)

def main_func(args):
    try:
        input_bucket=args['input_bucket']
        input_key=args['input_key']
        input_path=f's3://{input_bucket}/{input_key}'
        
        output_bucket= 'jsonfinals3'
        
        output_key= f'Processed_{input_key[:-5]}.parquet'
                
        #read the json using wrangler
        df=wr.s3.read_json(path=input_path)
        
        #change the df to a dictionary to pass to flatten
        dict_data=df.to_dict('records')
        
        #list to append flatted dictionaries to
        flattened_list=[]
        
        #iterate through each dictionary in the list of jsons
        for jsonobj in dict_data:
            flattened_list.append(flatten_json(jsonobj))
        
        #change the output dict to a df to write as a parquet
        output_df = pd.DataFrame(flattened_list)
        
        #remove the dollar sign from columns
        output_df.columns=output_df.columns.str.replace('$','')
       
        
        if flattened_list:
            s3_parquet_write(output_df, output_bucket, output_key)
            sns_notification(input_bucket,input_key,True)
    
    except FileNotFoundError:
        #send sns for input not FileNotFound
        sns_notification(input_bucket,input_key,False,'Input file not found!')

    except Exception as e:
        sns_notification(input_bucket,input_key,False,e)

    
if __name__=='__main__':
    args = getResolvedOptions(sys.argv, ['input_bucket', 'input_key' ])
    main_func(args)
    #time.sleep(60)
    #trigger_crawler()