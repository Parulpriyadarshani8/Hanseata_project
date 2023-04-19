
from azure.storage.blob import BlobClient
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv("Hanseata/.env") 
conn_string= os.getenv ("conn_string")


def move_data_to_blob():
     blob = BlobClient.from_connection_string(conn_str=conn_string, container_name="hanseata-from-airflow", blob_name="test-data-file2")
     read_csv_df = pd.read_csv("dags/Hanseata/testdaten.csv",on_bad_lines='skip')
     data=read_csv_df.to_csv(index=False)
     return blob.upload_blob(data)

#move_data_to_blob()

