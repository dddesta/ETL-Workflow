import sys
import time
import boto3
import awswrangler as wr
import pandas as pd
from awsglue.utils import getResolvedOptions

def start_crawler():
    glue=boto3.client('glue')
    glue.start_crawler(Name='etl-crawler')

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
        
        output_path=f's3://etl-final-destinationProcessed {input_key[:-4]}.parquet'
    
        #read csv from s3 using wrangler
        df=wr.s3.read_csv(input_path)
        
        #data dictionary ro transform data
        data_dict= {'Name': str,            # Assuming 'Name' is a string
            'Age': int,             # Assuming 'Age' is an integer
            'Height': float,        # Assuming 'Height' is a floating-point number
            'Gender': str,          # Assuming 'Gender' is a string
            'Date_of_Birth': str,   # Assuming 'Date_of_Birth' is a string representing a date
            'Weight': float,
            'Date_of_Birth': pd.to_datetime,  # Assuming 'Date_of_Birth' is a string representing a date
            'Registration_Date': pd.to_datetime,  # Assuming 'Registration_Date' is a string representing a date and time
            'Last_Activity_Date': pd.to_datetime  
        }
        
        #transform using pandas
        df=df.astype(data_dict)
        
        
        # success notif
        send_sns(input_bucket,input_key,True)

        # save as parquet
        wr.s3.to_parquet(df,output_path,compression='snappy')
    except FileNotFoundError:
        #failure notif with input file not found
        send_sns(input_bucket,input_key,False,'Input file not found')
    except Exception as e:
        # failure notif
        send_sns(input_bucket,input_key,False,str(e))

if __name__=='__main__':
    args=getResolvedOptions(sys.argv,['input_bucket','input_key'])
    main_func(args)
    
    time.sleep(60)
    start_crawler()
    