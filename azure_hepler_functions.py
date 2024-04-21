def create_container(conn_string, containers_name):
    from azure.storage.blob import ContainerClient
    container_client = ContainerClient.from_connection_string(conn_str=conn_string, containers_name= container_name)
    container_client.create_container()
    
def upload_blob(conn_string, containers_name, blobs_name, file_path):
    from azure.storage.blob import BlobClient
    blob = BlobClient.from_connection_string(conn_str=conn_string, container_name= containers_name, blob_name= blobs_name)
    with open(file_path, "rb") as data:
        blob.upload_blob(data)
        
def download_blob(conn_string, containers_name, blobs_name, file_path):
    from azure.storage.blob import BlobClient
    blob = BlobClient.from_connection_string(conn_str=conn_string, container_name= containers_name, blob_name=blobs_name)
    with open(file_path, "wb") as my_blob:
        blob_data = blob.download_blob()
        blob_data.readinto(my_blob)
        
def dir_files_to_list(file_path):
    import os
    # folder path
    dir_path = file_path
    # list to store files
    all_files = []
    # Iterate directory
    for path in os.listdir(dir_path):
        # check if current path is a file
        if os.path.isfile(os.path.join(dir_path, path)):
            all_files.append(path)
    return all_files

def create_connection_azure_sql(server_name, db_name, u_name, passw, driver_name):
    import pyodbc
    server = server_name
    database = db_name
    username = u_name
    password = passw 
    driver = driver_name

    with pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT @@Version")
            row = cursor.fetchall()
            print(row)
            
def create_table_in_azure_sql(server_name, db_name, u_name, passw, driver_name, query):
    import pyodbc
    server = server_name
    database = db_name
    username = u_name
    password = passw 
    driver = driver_name
    
    with pyodbc.connect('DRIVER= '+ driver + '; SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            
def execute_query(server_name, db_name, u_name, passw, driver_name, query):
    import pyodbc
    server = server_name
    database = db_name
    username = u_name
    password = passw 
    driver = driver_name
    
    with pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            
            
def fetch_data(server_name, db_name, u_name, passw, driver_name, data_fetching_query):
    import pyodbc
    server = server_name
    database = db_name
    username = u_name
    password = passw 
    driver = driver_name
    
    with pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password) as conn:
        with conn.cursor() as cursor:
            cursor.execute(data_fetching_query)
            row = cursor.fetchall()
            for i in range(1):
                print(row[i])
                
def dataframe_to_SQL_Server(server_name, db_name, u_name, passw, df, Target_table, Target_schema):
    import sqlalchemy as sa
    import urllib
    import pyodbc
    import pandas as pd

    server = server_name
    database = db_name
    username = u_name
    password = passw 
    driver= '{ODBC Driver 17 for SQL Server}'

    conn= urllib.parse.quote_plus('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    engine = sa.create_engine('mssql+pyodbc:///?odbc_connect={}'.format(conn))

    df.to_sql(Target_table, engine, schema=Target_schema, if_exists='append', index=False)
    
    
def blob_to_df(connection_string, container_name, blob_name):
    from azure.storage.blob import BlobServiceClient
    import pandas as pd
    import io
    
    # Connection string and container name
    connection_string = connection_string
    container_name = container_name
    blob_name = blob_name
    
    # Create a BlobServiceClient
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # Get the blob client
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    # Stream blob content directly into a DataFrame
    blob_data_stream = blob_client.download_blob().content_as_text()
    df = pd.read_csv(io.StringIO(blob_data_stream))

    return df
    
    
def blob_to_df_polars(connection_string, container_name, blob_name):
    from azure.storage.blob import BlobServiceClient
    import polars as pl
    import io
    
    # Connection string and container name
    connection_string = connection_string
    container_name = container_name
    blob_name = blob_name
    
    # Create a BlobServiceClient
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # Get the blob client
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    # Stream blob content directly into a DataFrame
    blob_data_stream = blob_client.download_blob().content_as_text()
    df = pl.read_csv(io.StringIO(blob_data_stream))

    return df
