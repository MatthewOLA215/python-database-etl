import mysql.connector
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

MYSQL_DB_USER = os.getenv("DB_USER")
MYSQL_PASSWORD = os.getenv("PASSWORD")
MYSQL_HOST = os.getenv("HOST")
MYSQL_PORT = os.getenv("PORT")
MYSQL_DATABASE = os.getenv("DATABASE")

PG_DB_USER = os.getenv("PG_DB_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DATABASE = os.getenv("PG_DATABASE")

def extract_from_mysql():
    db_uri = f"mysql+pymysql://{MYSQL_DB_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
    engine = create_engine(db_uri)
    query = """
        SELECT * 
        FROM sales_data 
        LIMIT 1000;
        """
    df = pd.read_sql(query, con=engine)
    print("Successfully Loaded from mysql !! ")
    print(df.head(10))
    # df.to_csv("data.csv", index= False)
    return df

def transorm_data(df):
    clothing_df = df[df['category'] == 'Clothing']
    clothing_df.to_csv('data2.csv', index= False)

    

def load_to_postgres(df):
    location_url = f"postgresql://{PG_DB_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
    engine = create_engine(location_url)
    df.to_sql(name="sales_data", con=engine, if_exists="replace")
    print("Successfully Loaded data into Postgres!")  
  


def main():
    extracted_data = extract_from_mysql()
    transorm_data(extracted_data)
    df = pd.read_csv("data2.csv")
    load_to_postgres(df)

if __name__ == "__main__":
    main()