# src/db_connect.py
import os
from dotenv import load_dotenv
import psycopg2
import pandas as pd
from src.utils import load_config

load_dotenv()

class RedshiftClient:
    def __init__(self, config_path="config.yaml"):
        config = load_config(config_path)
        self.sql_dirs = config['sql']
        self.host = os.getenv('DWH_HOST')
        self.port = os.getenv('DWH_PORT', 5439)
        self.db = os.getenv('DWH_DB')
        self.user = os.getenv('DWH_USER')
        self.password = os.getenv('DWH_PASSWORD')
        self.conn = None

    def connect(self):
        if not self.conn:
            self.conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.db,
                user=self.user,
                password=self.password
            )

    def run_query(self, sql_file: str, mode: str = "training") -> pd.DataFrame:
        """
        Executes a SQL file and returns a DataFrame.
        mode: 'training' or 'inference'
        """
        self.connect()
        if mode not in self.sql_dirs:
            raise ValueError(f"Invalid mode {mode}, must be one of {list(self.sql_dirs.keys())}")
        sql_path = os.path.join(self.sql_dirs[mode], sql_file)
        if not os.path.exists(sql_path):
            raise FileNotFoundError(f"SQL file not found: {sql_path}")
        with open(sql_path, 'r') as f:
            query = f.read()
        return pd.read_sql(query, self.conn)

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None