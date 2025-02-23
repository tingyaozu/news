import pypyodbc as odbc
import pandas as pd
import os
import re
from dotenv import load_dotenv

# Load environment variables locally (this will have no effect on GitHub Actions if env vars are provided via workflow)
load_dotenv()

# Build the connection string from environment variables
connection_string = (
    f"Driver={{ODBC Driver 18 for SQL Server}};"
    f"Server={os.getenv('DB_SERVER')},1433;"
    f"Database={os.getenv('DB_NAME')};"
    f"Uid={os.getenv('DB_USERNAME')};Pwd={os.getenv('DB_PASSWORD')};"
    f"Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
)

def normalize_title(title):
    """Normalize a title by stripping, converting to lowercase, and removing punctuation."""
    title = title.strip().lower()
    title = re.sub(r'[^\w\s]', '', title)
    return title

def read_sql(table_name):
    """Reads data from an Azure SQL table into a Pandas DataFrame using pypyodbc."""
    try:
        conn = odbc.connect(connection_string)
        query = f"SELECT * FROM {table_name}"
        existing_df = pd.read_sql_query(query, conn)
        conn.close()
        return existing_df
    except Exception as e:
        print(f"❌ Error reading SQL data: {e}")
        return pd.DataFrame()

def insert_news(news_article_df, news_table):
    """
    Inserts new news articles into the SQL database.

    Normalizes the 'Title' column in both the new DataFrame and the existing SQL data.
    Drops duplicates from the new data and inserts only rows with titles that do not exist
    in the SQL table.
    """
    # Normalize new data's Title column and drop duplicates within the new data
    news_article_df['Title'] = news_article_df['Title'].astype(str)
    news_article_df['NormalizedTitle'] = news_article_df['Title'].apply(normalize_title)
    news_article_df = news_article_df.drop_duplicates(subset=['NormalizedTitle'], keep='first')
    
    try:
        # Read existing data from SQL
        existing_df = read_sql(news_table)
        if not existing_df.empty and 'Title' in existing_df.columns:
            existing_df['NormalizedTitle'] = existing_df['Title'].astype(str).apply(normalize_title)
            normalized_existing = existing_df['NormalizedTitle'].tolist()
        else:
            normalized_existing = []
        
        # Filter new rows: keep only those whose normalized title is not in the existing data
        new_data = news_article_df[~news_article_df['NormalizedTitle'].isin(normalized_existing)]
        
        print("New data after filtering duplicates:")
        print(new_data[['Title', 'NormalizedTitle']])
        
        # Convert Published Date to string if needed
        if not new_data.empty and 'Published Date' in new_data.columns:
            new_data['Published Date'] = new_data['Published Date'].astype(str)
        
        if new_data.empty:
            print("⚠️ No new data to insert.")
            return
        
        # Prepare records for insertion (using original columns)
        records = list(new_data[['Title', 'News Hyperlinks', 'Published Date', 'Related Stocks']].itertuples(index=False, name=None))
        
        insert_query = f"""
            INSERT INTO {news_table} ([Title], [News Hyperlinks], [Published Date], [Related Stocks])
            VALUES (?, ?, ?, ?)
        """
        
        # Optional debugging: check character and byte lengths for each record
        max_lengths = {
            'Title': 255, 
            'News Hyperlinks': 255,  
            'Published Date': 50,
            'Related Stocks': 255     
        }
        columns = ['Title', 'News Hyperlinks', 'Published Date', 'Related Stocks']
        print("Checking each record before insertion:")
        for rec_num, record in enumerate(records, start=1):
            print(f"\nRecord {rec_num}:")
            for i, column in enumerate(columns):
                value = record[i]
                if isinstance(value, str):
                    char_length = len(value)
                    byte_length = len(value.encode('utf-8'))
                    print(f" - {column}: char length = {char_length}, byte length = {byte_length}")
                    if char_length > max_lengths[column]:
                        print(f"   ⚠️  Value in '{column}' exceeds max length: {char_length} > {max_lengths[column]}")
                elif value is None:
                    print(f" - {column}: None")
                else:
                    value_str = str(value)
                    char_length = len(value_str)
                    byte_length = len(value_str.encode('utf-8'))
                    print(f" - {column}: type = {type(value).__name__}, char length = {char_length}, byte length = {byte_length}")
        
        # Insert new records into SQL
        conn = odbc.connect(connection_string)
        cursor = conn.cursor()
        cursor.executemany(insert_query, records)
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"\n✅ Inserted {len(new_data)} new rows.")
    
    except Exception as e:
        print(f"❌ Error inserting data: {e}")
