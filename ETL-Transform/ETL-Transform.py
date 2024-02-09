import boto3
import sys
import awswrangler as wr
import pandas as pd
from awsglue.utils import getResolvedOptions

args=getResolvedOptions(sys.argv,['input_bucket','input_key'])

#send sns to send notifications to an sns topic. for success and failure
def send_sns(bucket,key,status=False,e=''):
    sns=boto3.client('sns')
    sub='CSV Transform Update!'
    
    if status:
        mssg=f'SUCCESS! The file {key} in {bucket} has been processed successfully'
    else:
        mssg=f'FAILURE! The file {key} in {bucket} has not been processed. \n \n Exception: {e} '
        
    sns.publish(
        TopicArn='arn:aws:sns:us-east-1:385363378908:ETLWorkFlowTopic',
        Message=mssg,
        Subject=sub
        )
    
# main function to extract load and transform the data
def main_func(args):
    try:
        input_bucket=args['input_bucket']
        input_key=args['input_key']
        input_path=f's3://{input_bucket}/{input_key}'
        
        output_path=f's3://etl-final-destination/processed{input_key[:-4]}.parquet'
    
        #read csv from s3 using wrangler
        df=wr.s3.read_csv(input_path)
        
        #data dictionary ro transform data
        data_dict= {'SITE_CM360':str, 'SITE_ID':str,
            'SITE_ID_SITE_DIRECTORY':'int',
            'SITE_SITE_DIRECTORY':str,
            'FILENAME':str,
            'FILE_ROW_NUMBER':'int'}
        
        #transform using pandas
        df=df.astype(data_dict)
        
        # change these columns to datetime
        df['PARTITION_DATE']= pd.to_datetime(df['PARTITION_DATE'])
        df['INSERT_DATETIME']= pd.to_datetime(df['INSERT_DATETIME'])
        
        
        # success notif
        send_sns(input_bucket,input_key,True)

        # save as parquet
        wr.s3.to_parquet(df,output_path,compression='snappy')
    
    except Exception as e:
        # failure notif
        send_sns(input_bucket,input_key,False,str(e))
        
main_func(args)
    