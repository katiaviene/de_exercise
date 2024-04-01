import pandas as pd
import sqlite3



def read_file(file):
    df = pd.read_csv(file)
    return df

def write_data(df, connection, table):
    df.to_sql(table, connection, if_exists="replace", index=False)

def init_db():
    conn = sqlite3.connect(':memory:')
    return conn

def read_query(query, connection):
    df = pd.read_sql(query, connection)
    return df
    
if __name__ == "__main__":
    df = read_file("data\priceplan_hierarchy_anonymized.csv")
    print(df)
    conn = init_db()
    write_data(df, conn, "priceplan")
    
    query = "SELECT COUNT(DISTINCT soc_pp_code) FROM priceplan"
    df1 = read_query(query, conn)
    print(df1)
    
    
    
    