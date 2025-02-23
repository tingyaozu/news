import pypyodbc as odbc
import pandas as pd
import os
from sqlalchemy import create_engine

# Azure SQL Database Connection (Private Access)
SERVER = os.getenv("DB_SERVER")
DATABASE = os.getenv("DB_NAME")
USERNAME = os.getenv("DB_USERNAME")
PASSWORD = os.getenv("DB_PASSWORD")

# Create connection string
connection_string = f'Driver={{ODBC Driver 18 for SQL Server}};Server={SERVER},1433;Database={DATABASE};Uid={USERNAME};Pwd={PASSWORD};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'

def read_sql(table_name):
    """Reads data from Azure SQL table into a Pandas DataFrame."""
    try:
        engine = create_engine(
            f"mssql+pyodbc://{USERNAME}:{PASSWORD}@{SERVER}/{DATABASE}?driver=ODBC+Driver+18+for+SQL+Server"
        )
        query = f"SELECT * FROM {table_name}"
        with engine.connect() as conn:
            existing_df = pd.read_sql(query, conn)
        return existing_df
    except Exception as e:
        print(f"❌ Error reading SQL data: {e}")
        return pd.DataFrame()

def insert_news(news_article_df, news_table):
    """Inserts new news articles into the SQL database, avoiding duplicates."""
    try:
        existing_titles = read_sql(news_table)['Title'].tolist()
        
        # Filter out duplicate Titles before inserting
        new_data = news_article_df[~news_article_df['Title'].isin(existing_titles)]
        
        if not new_data.empty:
            records = list(new_data.itertuples(index=False, name=None))
            
            insert_query = f"""
            INSERT INTO {news_table} ([Title], [News Hyperlinks], [Published Date], [Related Stocks])
            VALUES (?, ?, ?, ?)
            """
            
            with odbc.connect(connection_string) as conn:
                with conn.cursor() as cursor:
                    cursor.executemany(insert_query, records)
                    conn.commit()
            
            print(f"✅ Inserted {len(new_data)} new rows.")
        else:
            print("⚠️ No new data to insert.")
    
    except Exception as e:
        print(f"❌ Error inserting data: {e}")

