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
        # Warning: pandas warns that only certain connection types are tested.
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
        
        # Convert the Published Date column from string to datetime if necessary,
        # then convert pandas Timestamp to native Python datetime.
        if not new_data.empty and 'Published Date' in new_data.columns:
            new_data['Published Date'] = pd.to_datetime(new_data['Published Date'], errors='coerce')
            new_data['Published Date'] = new_data['Published Date'].apply(lambda x: x.to_pydatetime() if pd.notnull(x) else None)
        
        if not new_data.empty:
            records = list(new_data.itertuples(index=False, name=None))
            
            insert_query = f"""
            INSERT INTO {news_table} ([Title], [News Hyperlinks], [Published Date], [Related Stocks])
            VALUES (?, ?, ?, ?)
            """
            
            # Set your maximum allowed lengths for each column.
            # For datetime fields, our check will now show the type.
            max_lengths = {
                'Title': 255, 
                'News Hyperlinks': 255,  
                'Published Date': 50,    # This limit is only used for string debugging.
                'Related Stocks': 255     
            }
            # Order of columns in the insert query:
            columns = ['Title', 'News Hyperlinks', 'Published Date', 'Related Stocks']

            # Print out the length or type of each value before insertion.
            print("Checking each record before insertion:")
            for rec_num, record in enumerate(records, start=1):
                print(f"\nRecord {rec_num}:")
                for i, column in enumerate(columns):
                    value = record[i]
                    # For datetime objects, print type and value.
                    if column == 'Published Date':
                        print(f" - {column}: type = {type(value).__name__}, value = {value}")
                    elif isinstance(value, str):
                        value_length = len(value)
                        print(f" - {column}: length = {value_length}")
                        if value_length > max_lengths[column]:
                            print(f"   ⚠️  Value in '{column}' exceeds max length: {value_length} > {max_lengths[column]}")
                    elif value is None:
                        print(f" - {column}: None")
                    else:
                        print(f" - {column}: type = {type(value).__name__}, value = {value}")

            # Proceed with the insert operation.
            conn = odbc.connect(connection_string)
            cursor = conn.cursor()
            cursor.executemany(insert_query, records)
            conn.commit()
            cursor.close()
            conn.close()
            
            print(f"\n✅ Inserted {len(new_data)} new rows.")
        else:
            print("⚠️ No new data to insert.")
    
    except Exception as e:
        print(f"❌ Error inserting data: {e}")

# Example usage:
# news_df = pd.DataFrame({...})
# insert_news(news_df, 'YourNewsTable')
