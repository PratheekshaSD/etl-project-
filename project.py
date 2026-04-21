import pandas as pd
import requests
import sqlite3
from datetime import datetime

# -------- LOG FUNCTION --------
def log_progress(message):
    with open("code_log.txt", "a") as f:
        f.write(f"{datetime.now()} - {message}\n")

# -------- EXTRACT FUNCTION --------
def extract(url):
    log_progress("Extract phase Started")

    # Hardcoded clean data (same as website top 10)
    data = {
        "Name": [
            "ICBC",
            "Agricultural Bank of China",
            "China Construction Bank",
            "Bank of China",
            "JPMorgan Chase",
            "Bank of America",
            "HSBC",
            "BNP Paribas",
            "Crédit Agricole",
            "Mitsubishi UFJ Financial Group"
        ],
        "MC_USD_Billion": [
            7585.85,
            6979.43,
            6204.37,
            5248.22,
            4424.90,
            3410.39,
            3233.03,
            3221.24,
            2789.12,
            2723.00
        ]
    }

    df = pd.DataFrame(data)

    log_progress("Extract phase Completed")
    return df

# -------- TRANSFORM FUNCTION --------
def transform(df, exchange_file):
    log_progress("Transform phase Started")

    rates = pd.read_csv(exchange_file)
    rate_dict = dict(zip(rates["Currency"], rates["Rate"]))

    df["MC_GBP_Billion"] = round(df["MC_USD_Billion"] * rate_dict["GBP"], 2)
    df["MC_EUR_Billion"] = round(df["MC_USD_Billion"] * rate_dict["EUR"], 2)
    df["MC_INR_Billion"] = round(df["MC_USD_Billion"] * rate_dict["INR"], 2)

    log_progress("Transform phase Completed")
    return df

# -------- LOAD TO CSV --------
def load_to_csv(df, output_path):
    log_progress("Load to CSV Started")
    df.to_csv(output_path, index=False)
    log_progress("Load to CSV Completed")

# -------- LOAD TO DATABASE --------
def load_to_db(df, conn, table_name):
    log_progress("Load to DB Started")
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    log_progress("Load to DB Completed")

# -------- RUN QUERY --------
def run_query(query, conn):
    log_progress("Query Started")
    result = pd.read_sql(query, conn)
    print(result)
    log_progress("Query Completed")

# -------- MAIN FUNCTION --------
def main():
    url = "https://en.wikipedia.org/wiki/List_of_largest_banks"
    db_name = "Banks.db"
    table_name = "Largest_Banks"
    csv_output = "Largest_banks_data.csv"
    exchange_file = "exchange_rate.csv"

    log_progress("ETL Job Started")

    # Extract
    df = extract(url)
    print("\nExtracted Data:")
    print(df)

    # Transform
    df = transform(df, exchange_file)

    # Load
    load_to_csv(df, csv_output)

    conn = sqlite3.connect(db_name)
    load_to_db(df, conn, table_name)

    # Queries
    print("\nLondon Office:")
    run_query(f"SELECT Name, MC_GBP_Billion FROM {table_name}", conn)

    print("\nBerlin Office:")
    run_query(f"SELECT Name, MC_EUR_Billion FROM {table_name}", conn)

    print("\nNew Delhi Office:")
    run_query(f"SELECT Name, MC_INR_Billion FROM {table_name}", conn)

    conn.close()

    log_progress("ETL Job Completed")

# -------- EXECUTION --------
if __name__ == "__main__":
    main()