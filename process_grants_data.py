#!/usr/bin/env python3

import requests
import boto3
from datetime import datetime
import zipfile
import xml.etree.ElementTree as ET
import pandas as pd
from io import BytesIO

def process_grants_data():
    # Setup S3 client and parameters
    s3 = boto3.client('s3')
    bucket_name = 'grantsgov'
    prefix = 'GrantsDBExtract/'
    
    # Get the current date in the specified format
    current_date = datetime.now().strftime("%Y%m%d")
    
    # Delete previous XML files from S3
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    xml_files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.xml')]
    for xml_file_key in xml_files:
        s3.delete_object(Bucket=bucket_name, Key=xml_file_key)
        print(f"Deleted {xml_file_key} from S3.")
    
    # Delete previous Parquet files from S3
    parquet_files = [obj['Key'] for obj in s3.list_objects_v2(Bucket=bucket_name).get('Contents', []) if obj['Key'].endswith('.parquet')]
    for parquet_file_key in parquet_files:
        s3.delete_object(Bucket=bucket_name, Key=parquet_file_key)
        print(f"Deleted {parquet_file_key} from S3.")
    
    # Download the latest grants ZIP file
    current_date = datetime.now().strftime("%Y%m%d")
    file_name = f"GrantsDBExtract{current_date}v2.zip"
    file_url = f"https://prod-grants-gov-chatbot.s3.amazonaws.com/extracts/{file_name}"
    file_key = f"GrantsDBExtract/{file_name}"
    response = requests.get(file_url)
    if response.status_code == 200:
        print("File downloaded successfully.")
        s3.put_object(Bucket=bucket_name, Key=file_key, Body=response.content)
        print(f"File uploaded successfully to S3 bucket '{bucket_name}' with key '{file_key}'.")
    else:
        raise Exception("Failed to download the file.")
    
    # Unzip and upload files
    unzip_folder = 'GrantsDBExtract/unzip/'
    zip_files = [obj['Key'] for obj in s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix).get('Contents', []) if obj['Key'].endswith('.zip')]
    for zip_file_key in zip_files:
        zip_obj = s3.get_object(Bucket=bucket_name, Key=zip_file_key)
        with zipfile.ZipFile(BytesIO(zip_obj['Body'].read()), 'r') as zip_ref:
            for file_name in zip_ref.namelist():
                file_data = zip_ref.read(file_name)
                s3_file_key = f"{unzip_folder}{file_name}"
                s3.put_object(Bucket=bucket_name, Key=s3_file_key, Body=file_data)
        s3.delete_object(Bucket=bucket_name, Key=zip_file_key)
        print(f"Processed and deleted {zip_file_key}")
    
    # Parse the most recent XML file
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=unzip_folder)
    files = sorted([obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.xml')])
    most_recent_file = files[-1]
    obj = s3.get_object(Bucket=bucket_name, Key=most_recent_file)
    xml_data = obj['Body'].read()
    namespaces = {'g': 'http://apply.grants.gov/system/OpportunityDetail-V1.0'}
    tree = ET.parse(BytesIO(xml_data))
    root = tree.getroot()
    extracted_data = []
    for opportunity in root.findall('.//g:OpportunitySynopsisDetail_1_0', namespaces):
        record = {field: opportunity.find(f'.//g:{field}', namespaces).text if opportunity.find(f'.//g:{field}', namespaces) is not None else None 
                  for field in ['OpportunityID', 'OpportunityTitle', 'Description','AgencyCode','OpportunityNumber', 'OpportunityCategory', 
                  'FundingInstrumentType', 'CFDANumbers', 'CategoryOfFundingActivity', 
                  'EligibleApplicants', 'AdditionalInformationOnEligibility', 'AgencyName', 
                  'PostDate', 'CloseDate', 'LastUpdatedDate', 'AwardCeiling',
                  'EstimatedTotalProgramFunding', 'ExpectedNumberOfAwards']}
        extracted_data.append(record)
    df = pd.DataFrame(extracted_data)
    print(f"Number of records matching criteria: {len(df)}")

    # Convert dates and save as Parquet
    df['CloseDate'] = pd.to_datetime(df['CloseDate'], format='%m%d%Y')
    df['PostDate'] = pd.to_datetime(df['PostDate'], format='%m%d%Y')
    df['LastUpdatedDate'] = pd.to_datetime(df['LastUpdatedDate'], format='%m%d%Y')
    # Define the path for saving the Parquet file with the date appended
    s3_path = f's3://grantsgov/GrantsExtract{current_date}v2.parquet'
    # s3_path = 's3://grantsgov/df.parquet'
    # Save the DataFrame as a Parquet file
    df.to_parquet(s3_path, engine='pyarrow', index=False)
    print(f"DataFrame is saved as Parquet to {s3_path}")

# Run the function
if __name__ == "__main__":
    process_grants_data()