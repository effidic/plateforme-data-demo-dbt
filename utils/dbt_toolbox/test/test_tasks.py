import unittest
import os
import sys
import shutil
import snowflake.connector
import subprocess

curr_path = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(curr_path, os.path.pardir))
from database_util import DatabaseUtil
from global_util import DBT_BASE_PATH, DBT_MODELS_PATH, DBT_FLAG_BUILD, DBT_FLAG_DOC



class TestTask(unittest.TestCase):
    """Goal: test commands from tasks.py file and help to understand command behaviour
    
    python test/test_tasks.py
    """
    db_conn_info = DatabaseUtil.get_db_conn_info()
    SNOWFLAKE_DB_SOURCE = db_conn_info["database"]
    SNOWFLAKE_SCHEMA_SOURCE = db_conn_info["schema"]
    SNOWFLAKE_DB_MODEL = db_conn_info["database"]
    SNOWFLAKE_SCHEMA_MODEL = db_conn_info["schema"]

    @classmethod
    def setUpClass(cls):
        # Change working directory
        os.chdir(DBT_BASE_PATH)
        # Init Snowflake db schema/tables
        cls.conn = snowflake.connector.connect(
            user=cls.db_conn_info["user"],
            authenticator='externalbrowser',
            account=cls.db_conn_info["account"],
            warehouse=cls.db_conn_info["warehouse"],
            role=cls.db_conn_info["role"],
            database=cls.SNOWFLAKE_DB_SOURCE,
            schema=cls.SNOWFLAKE_SCHEMA_SOURCE,
        )
        cur = cls.conn.cursor()
        cur.execute(f'CREATE OR REPLACE TABLE {cls.SNOWFLAKE_DB_SOURCE}.{cls.SNOWFLAKE_SCHEMA_SOURCE}.test_source (col1 integer, col2 string)')
        cur.close()
        # Create dbt models/sources
        shutil.copy2(os.path.join(curr_path, 'test_model.sql'), os.path.join(DBT_MODELS_PATH, 'test_model.sql'))



    @classmethod
    def tearDownClass(cls):
        print('tearDownClass')
        # clean Snowflake db schema/tables
        cur = cls.conn.cursor()
        cur.execute(f'DROP TABLE {cls.SNOWFLAKE_DB_SOURCE}.{cls.SNOWFLAKE_SCHEMA_SOURCE}.test_source')
        cur.close()
        cls.conn.close()
        # Remove dbt models/sources
        os.remove(os.path.join(DBT_MODELS_PATH, 'test_model.sql'))


    def test_compile_all(self):
        cmd = 'invoke compile-all'
        p = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.assertEqual(p.returncode, 0, f'Error command:\n{p.stdout}\n{p.stderr}')


    def test_compile_model(self):
        cmd = 'invoke compile-model test_model'
        p = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.assertEqual(p.returncode, 0, f'Error command:\n{p.stdout}\n{p.stderr}')


    def test_run_model(self):
        try:
            cmd = 'invoke run-model test_model'
            p = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            self.assertEqual(p.returncode, 0, f'Error command:\n{p.stdout}\n{p.stderr}')
            # Check if the file to flag the run-model is created (and clean it)
            flag_path = os.path.join(DBT_FLAG_BUILD, 'test_model.sql.md5')
            self.assertTrue(os.path.exists(flag_path), 'The flag file does not exist')
        finally: # Clean files
            if flag_path and os.path.exists(flag_path):
                os.remove(flag_path)


    def test_doc_model(self):
        try:
            p = subprocess.run('invoke doc-model test_model', shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            self.assertEqual(p.returncode, 0, f'Error command:\n{p.stdout}\n{p.stderr}')
            # Check if the files to flag the doc-model are created (and clean it)
            flag_sql_path = os.path.join(DBT_FLAG_DOC, 'test_model.sql.md5')
            flag_yml_path = os.path.join(DBT_FLAG_DOC, 'test_model.yml.md5')
            self.assertTrue(os.path.exists(flag_sql_path), 'The SQL flag file does not exist')
            self.assertTrue(os.path.exists(flag_yml_path), 'The SQL YML file does not exist')
            # Check generated .yml file content
            expected_test_model_yml_path = os.path.join(curr_path, 'expected_test_model.yml')
            actual_test_model_yml_path = os.path.join(DBT_MODELS_PATH, 'test_model.yml')
            with open(expected_test_model_yml_path, 'r') as f:
                expected_test_model_yml = f.read()
            with open(actual_test_model_yml_path, 'r') as f:
                actual_test_model_yml = f.read()
            self.assertEqual(actual_test_model_yml, expected_test_model_yml, 'Generated .yml model - bad format')
        finally: # Clean files
            if flag_sql_path and os.path.exists(flag_sql_path):
                os.remove(flag_sql_path)
            if flag_yml_path and os.path.exists(flag_yml_path):
                os.remove(flag_yml_path)
            if actual_test_model_yml_path and os.path.exists(actual_test_model_yml_path):
                os.remove(actual_test_model_yml_path) 


    def test_doc_source(self):
        try:
            cmd = f'invoke doc-source {self.SNOWFLAKE_DB_SOURCE}.{self.SNOWFLAKE_SCHEMA_SOURCE}.test_source'
            p = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            self.assertEqual(p.returncode, 0, f'Error command:\n{p.stdout}\n{p.stderr}')
            # Check generated .yml file content
            expected_test_source_yml_path = os.path.join(curr_path, 'expected_test_source.yml')
            actual_test_source_yml_path = os.path.join(DBT_MODELS_PATH, '0_sources', self.SNOWFLAKE_SCHEMA_SOURCE, 'src_test_source.yml')
            with open(expected_test_source_yml_path, 'r') as f:
                expected_test_source_yml = f.read()
            with open(actual_test_source_yml_path, 'r') as f:
                actual_test_source_yml = f.read()
            self.assertEqual(actual_test_source_yml, expected_test_source_yml, 'Generated .yml source - bad format')
        finally: # Clean files
            src_test_folder = os.path.join(DBT_MODELS_PATH, '0_sources', self.SNOWFLAKE_SCHEMA_SOURCE)
            if os.path.exists(src_test_folder):
                shutil.rmtree(src_test_folder)


if __name__ == "__main__":
    unittest.main()
