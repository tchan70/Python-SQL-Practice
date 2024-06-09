import os
import requests
import pandas as pd
import psycopg2
from psycopg2 import sql

# Extract
url = "https://yahoo-finance127.p.rapidapi.com/key-statistics/aapl"
headers = {
    "x-rapidapi-key": "26e97ffc1cmsh57bb80fee2686cep1aebdbjsn7380f6dc9061",
    "x-rapidapi-host": "yahoo-finance127.p.rapidapi.com"
}

response = requests.get(url, headers=headers)
responseData = response.json()

print(responseData)

# Transform
data = {
    "symbol": responseData.get("symbol"),
    "dividend_date": pd.to_datetime(responseData['dividendDate']['fmt']),
    "dividend_yield": responseData['dividendYield']['raw'],
    "two_hundred_day_avg_change_percent": responseData['twoHundredDayAverageChangePercent']['raw'],
    "market_cap": responseData['marketCap']['raw'],
}

df = pd.DataFrame([data])
print(df)

# Load
conn = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
)

cur = conn.cursor()

create_table_query = """
CREATE TABLE IF NOT EXISTS stock_data (
    symbol VARCHAR(10),
    dividend_date TIMESTAMP,
    dividend_yield FLOAT,
    two_hundred_day_avg_change_percent FLOAT,
    market_cap BIGINT
);
"""

cur.execute(create_table_query)
conn.commit()

insert_query = sql.SQL("""
INSERT INTO stock_data ({})
VALUES ({})
""").format(
    sql.SQL(', ').join(map(sql.Identifier, data.keys())),
    sql.SQL(', ').join(sql.Placeholder() * len(data))
)

cur.execute(insert_query, list(data.values()))
conn.commit()

cur.close()
conn.close()

print("Data inserted successfully!")
