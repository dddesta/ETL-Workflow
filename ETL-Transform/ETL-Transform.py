import awswrangler as wr
import boto3
import pandas as pd

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
def main_func():
    try:
        input_bucket='s3-etlproject-drop'
        input_key='data.csv'
        input_path='s3://s3-etlproject-drop/data.csv'
        
        output_path='s3://etl-final-destination/csvout.parquet'
    
        #read csv from s3 using wrangler
        df=wr.s3.read_csv(input_path)
        
        #data dictionary ro transform data
        data_dict= {'SITE_CM360':str, 'SITE_ID':str,
            'SITE_ID_SITE_DIRECTORY':'int',
            'SITE_SITE_DIRECTORY':str,
            'FILENAME':str,
            'FILE_ROW_NUMBER':'int'}
        
        #transform using pandas
        df.astype(data_dict)
        
        # change these columns to datetime
        df['PARTITION_DATE']= pd.to_datetime(df['PARTITION_DATE'])
        df['INSERT_DATETIME']= pd.to_datetime(df['INSERT_DATETIME'])
        
        
        # success notif
        send_sns(input_bucket,input_key,True)

        # save as parquet
        wr.s3.to_parquet(df, output_path,compression='snappy')
    
    except Exception as e:
        # failure notif
        send_sns(input_bucket,input_key,False,str(e))
        

def lambda_handler(event, context):
    # TODO implement
    main_func()
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
    