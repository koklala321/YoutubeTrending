import pandas as pd
import awswrangler as wr
from datetime import date,timedelta

def csv_transform(input,output,country):
    try:
        df = wr.s3.read_csv(path = input)
        
        end_date = date.today()
        period = 180
        start_date = end_date - timedelta(days=period)
        
        #add country column
        df["country"] = country
        
        #remove useless column
        df = df.drop("thumbnail_link",axis =1)
    
        #trim publish date
        df['publishedAt'] = pd.to_datetime(df['publishedAt'], errors='coerce', format='%Y-%m-%dT%H:%M:%S%fZ').dt.date
        #trim trending date
        df['trending_date'] = pd.to_datetime(df['trending_date'], errors='coerce', format='%Y-%m-%dT%H:%M:%S%fZ').dt.date
        
        df = df[(df["trending_date"] >= start_date ) & (df["trending_date"] <= end_date)]
    
        df = df.sort_values(by=["trending_date"]) 
        
        wr.s3.to_csv(df,path =output, index = False)
        
        #remove raw file
        wr.s3.delete_objects(input)
    
        return {
            "status": "completed"
        }
    except Exception as e:
        raise e



def lambda_handler(event, context):
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        raw_key = record['s3']['object']['key']
        #testing bucket/key
        #bucket = 'hytsaibucket'
        #raw_key = 'Raw/test.csv'
        country = raw_key[4:6]
        processed_key = f'Processed/{country}_processed.csv'
        inputPath = f"s3://{bucket}/{raw_key}"
        outputPath = f"s3://{bucket}/{processed_key}"
        csv_transform(inputPath,outputPath,country)

    return {
        "status": "completed"
    }
    

    

    

    

