import boto3
import pandas as pd
import fastparquet

def main():

#Connection String to s3 bucket
region = "us-east-2"
bucket = 'blossom-data-eng-elvis'
s3_client = boto3.client('s3', region_name=region)

#Reading the file
df = pd.read_csv("companies_sorted.csv")

#filtering the csv file
 filtered_domain = df[df.domain.notnull()]

#saving the files in json and parquet
filtered_domain.to_parquet('free-7-million-company-dataset.parquet', compression='gzip')
filtered_domain.to_json('free-7-million-company-dataset-json.gzip', compression='gzip')

#Uploading the files(json & parquet)
s3_client.upload_file( "free-7-million-company-dataset.parquet", bucket, "project1/free-7-million-company-dataset.parquet")
s3_client.upload_file("free-7-million-company-dataset-json.gzip", bucket, "project1/free-7-million-company-dataset-json.gzip")

if __name__ == "__main__":
	main()