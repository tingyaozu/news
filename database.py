import pypyodbc as odbc
import pandas as pd
import os

# Azure SQL Database Connection (Private Access)
SERVER = os.getenv("DB_SERVER")
DATABASE = os.getenv("DB_NAME")
USERNAME = os.getenv("DB_USERNAME")
PASSWORD = os.getenv("DB_PASSWORD")

# Create connection string
connection_string = (
    f"Driver={{ODBC Driver 18 for SQL Server}};"
    f"Server={SERVER},1433;"
    f"Database={DATABASE};"
    f"Uid={USERNAME};Pwd={PASSWORD};"
    f"Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
)

def read_sql(table_name):
    """Reads data from an Azure SQL table into a Pandas DataFrame using pypyodbc."""
    try:
        conn = odbc.connect(connection_string)
        query = f"SELECT * FROM {table_name}"
        # Use Pandas' read_sql_query with the ODBC connection
        existing_df = pd.read_sql_query(query, conn)
        conn.close()
        return existing_df
    except Exception as e:
        print(f"❌ Error reading SQL data: {e}")
        return pd.DataFrame()

def insert_news(news_article_df, news_table):
    """Inserts new news articles into the SQL database, avoiding duplicates."""
    try:
        existing_df = read_sql(news_table)
        if existing_df.empty or 'Title' not in existing_df.columns:
            existing_titles = []
        else:
            existing_titles = existing_df['Title'].tolist()
        
        # Filter out duplicate Titles before inserting
        new_data = news_article_df[~news_article_df['Title'].isin(existing_titles)]
        
        if not new_data.empty:
            records = list(new_data.itertuples(index=False, name=None))
            
            insert_query = f"""
            INSERT INTO {news_table} ([Title], [News Hyperlinks], [Published Date], [Related Stocks])
            VALUES (?, ?, ?, ?)
            """
            
            conn = odbc.connect(connection_string)
            cursor = conn.cursor()
            cursor.executemany(insert_query, records)
            conn.commit()
            cursor.close()
            conn.close()
            
            print(f"✅ Inserted {len(new_data)} new rows.")
        else:
            print("⚠️ No new data to insert.")
    
    except Exception as e:
        print(f"❌ Error inserting data: {e}")
