import yaml
import psycopg2
from sqlalchemy import create_engine
import pandas as pd
from sqlalchemy import inspect

DBAPI = 'psycopg2'

class DatabaseConnector:
    def __init__(self):
        self.read_db_creds()
        self.init_db_engine()
    
    def read_db_creds(self):
        with open('db_creds.yml', 'r') as creds:
            self.data_loaded = yaml.safe_load(creds)
            psycopg2.connect(host  = self.data_loaded['RDS_HOST'], user = self.data_loaded['RDS_USER'], password = self.data_loaded['RDS_PASSWORD'], dbname = self.data_loaded['RDS_DATABASE'], port = 5432)
            
    def init_db_engine(self):
        self.engine = create_engine(f"{'postgresql'}+{DBAPI}://{self.data_loaded['RDS_USER']}:{self.data_loaded['RDS_PASSWORD']}@{self.data_loaded['RDS_HOST']}:{self.data_loaded['RDS_PORT']}/{self.data_loaded['RDS_DATABASE']}")
        self.engine.connect()
        
    def list_db_tables(self):
        inspector = inspect(self.engine)
        tables = inspector.get_table_names()
        return tables

    def upload_to_db(self, df, table):
        database_type = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = 'localhost'
        USER = 'postgres'
        PASSWORD = 'OllieMountier'
        DATABASE = 'Sales_Data'
        PORT = 5432
      
        self.sql_engine = create_engine(f"{database_type}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        with self.sql_engine.connect().execution_options(autocommit=True) as conn:
            df.to_sql(table, con = conn, if_exists = 'replace', index = False)




#dbc = DatabaseConnector()
# dbc.read_db_creds()
# dbc.init_db_engine()
# dbc.list_db_tables()








