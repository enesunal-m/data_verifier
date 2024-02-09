import requests
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

def fetch_binance_data(symbol, interval, start_time, end_time):
    base_url = "https://api.binance.com/api/v3/klines"
    params = {
        'symbol': symbol,
        'interval': interval,
        'startTime': int(pd.Timestamp(start_time).timestamp() * 1000),
        'endTime': int(pd.Timestamp(end_time).timestamp() * 1000),
        'limit': 10  # Adjust based on your needs
    }
    response = requests.get(base_url, params=params)
    response.raise_for_status()

    # Convert the API response into a DataFrame
    columns = ['Open Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close Time', 'Quote Asset Volume', 'Number of Trades', 'Taker Buy Base Asset Volume', 'Taker Buy Quote Asset Volume', 'Ignore']
    data = pd.DataFrame(response.json(), columns=columns)
    data['Open Time'] = pd.to_datetime(data['Open Time'], unit='ms')
    data['Close Time'] = pd.to_datetime(data['Close Time'], unit='ms')
    return data

def fetch_database_data(conn_details, view_name, start_time, end_time, symbol):
    conn_str = f"postgresql+psycopg2://{conn_details['user']}:{conn_details['password']}@{conn_details['host']}:{conn_details['port']}/{conn_details['dbname']}"
    engine = create_engine(conn_str)

    query = f"""
    SELECT * FROM {view_name}
    WHERE bucket >= %s AND bucket < %s and tickersymbol = %s
    ORDER BY bucket ASC;
    """
    print(start_time, end_time)
    # Use SQLAlchemy to execute the query and fetch data into a DataFrame
    with engine.connect() as connection:
        df = pd.read_sql_query(query, connection, params=(start_time, end_time, symbol))

    return df



def compare_data(df_binance, df_database):
    # Assuming 'Open Time' in Binance data corresponds to 'time' in database data
    # You may need to adjust the logic based on how your data is aggregated

    # Convert time columns to the same format if necessary
    df_binance['Open Time'] = df_binance['Open Time'].dt.floor('Min')
    df_database['bucket'] = pd.to_datetime(df_database['bucket']).dt.floor('Min')

    # Merge datasets on the time column to find matching rows
    df_merged = pd.merge(df_binance, df_database, left_on='Open Time', right_on='bucket', how='outer', indicator=True)

    # Filter out rows that don't match
    mismatches = df_merged[df_merged['_merge'] != 'both']
    return mismatches

def main():
    load_dotenv

    symbol = 'BTCUSDT'  # Example symbol
    interval = '3m'  # 1-minute interval
    start_time = '2024-02-09 01:00:00'
    end_time = '2024-02-09 01:30:00'
    view_name = 'candle_3m'
    conn_details = {'dbname': os.getenv("DB_NAME"), 
                    'user': os.getenv("DB_USER"), 
                    'password': os.getenv("DB_PASSWORD"), 
                    'host': os.getenv("DB_HOST"), 
                    'port': os.getenv("DB_PORT")}

    # Fetch data from Binance
    df_binance = fetch_binance_data(symbol, interval, start_time, end_time)

    # Fetch data from your database
    df_database = fetch_database_data(conn_details, view_name, start_time, end_time, symbol)

    # Compare the data
    mismatches = compare_data(df_binance, df_database)

    if not mismatches.empty:
        print("Discrepancies found:")
        print(mismatches)
    else:
        print("No discrepancies found.")

if __name__ == "__main__":
    main()
