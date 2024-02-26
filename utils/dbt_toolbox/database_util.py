from typing import List, Dict
import os
import yaml
import shutil
import subprocess
from google.cloud import bigquery
from google.oauth2 import service_account
import time

from global_util import DBT_BASE_PATH, DBT_PROJECT_NAME, DBT_TARGET_PATH, DBT_PATH_VENV
from global_util import logger


class DatabaseUtil:
    """DataBase util"""

    @staticmethod
    def get_db_connection() -> bigquery.Client:
        service_account_info = {
            "type": "service_account",
            "project_id": os.environ.get('KEYFILE_PROJECT_ID'),
            "private_key_id": os.environ.get('KEYFILE_PRIVATE_KEY_ID'),
            "private_key": os.environ.get('KEYFILE_PRIVATE_KEY'),
            "client_email": os.environ.get('KEYFILE_CLIENT_EMAIL'),
            "client_id": os.environ.get('KEYFILE_CLIENT_ID'),
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": os.environ.get('KEYFILE_CLIENT_X509_CERT_URL')
        }
        credentials = service_account.Credentials.from_service_account_info(service_account_info) 
        client = bigquery.Client(credentials=credentials, project=os.environ.get('KEYFILE_PROJECT_ID'))
        print(os.environ.get('KEYFILE_PROJECT_ID'))
        return client

    @staticmethod
    def get_table_name_columns(
        db_database: str, db_schema: str, db_table: str
    ) -> List[str]:
        """Return column names for a the Bigquery table"""
        logger.debug(
            {"db_database": db_database, "db_schema": db_schema, "db_table": db_table}
        )
        name_columns = []
    
        client = DatabaseUtil.get_db_connection()
        try:
            query_str=f"select column_name from {db_database}.{db_schema}.INFORMATION_SCHEMA.COLUMNS where table_name = '{db_table}'"
            logger.debug(query_str)
            rows = client.query(query_str).result() 
            for row in rows:
                name_columns.append(row[0])
        finally:
            client.close()
        name_columns.sort()
        logger.debug(f"name_columns: {name_columns}")
        return name_columns
    
    @staticmethod
    def bulk_get_table_name_columns( models ) :
        """Return column names for a the Bigquery table"""
        logger.debug( f"models : {models}"  )
        client = DatabaseUtil.get_db_connection()
        for model in models:
            resource_type = model['resource_type']  
            table_name = model['name']  
            if resource_type == 'snapshot':
                db_schema = model['config']['target_schema']
                db_database = model['config']['target_database']
            else:
                db_schema = model['config']['schema']
                db_database= os.environ.get('KEYFILE_PROJECT_ID')
            query_str=f"select column_name from {db_database}.{db_schema}.INFORMATION_SCHEMA.COLUMNS where table_name = '{table_name}'"
            logger.debug(query_str)
            results = client.query(query_str).result() 
            column_names = [row[0] for row in results]
            column_names.sort()
            model['retrieved_columns'] = column_names
        client.close()
        
        return models

    
    @staticmethod
    def bulk_columns_ephemeral( models ):
        """
        Getting columns for  materialized : ephemeral
               
        """
        shutil.rmtree(DBT_TARGET_PATH, ignore_errors=True)  # Handle strange behaviour
        cmd_model_list = " ".join(model['name'] for model in models)
        logger.debug(cmd_model_list)
        cmd = f'"{DBT_PATH_VENV}" compile --select {cmd_model_list} '
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode != 0:
          raise Exception(f"Error during runing dbt command:\n{result}")
        #logger.debug({"result.stdout": result.stdout})
        client = DatabaseUtil.get_db_connection()        
        for model in models:
            compiled_path = os.path.join(DBT_TARGET_PATH,'compiled',DBT_PROJECT_NAME,model['original_file_path'])
            #logger.debug(f"compiled sql for {model} is {compiled_path}")
            with open(compiled_path, 'r') as file:
                sql_query = file.read()
                results = client.query(sql_query).result() 
                column_names = [field.name for field in results.schema]
                column_names.sort()
                #logger.debug(column_names)
                model['retrieved_columns'] = column_names
        client.close()
        #logger.debug(models)
        return models
    