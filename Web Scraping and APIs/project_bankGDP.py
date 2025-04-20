# Code for ETL operations on Largest Banks Market Cap data

# Importing the required libraries
import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from io import StringIO

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%m-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open("./code_log.txt", "a") as f:
        f.write(timestamp + ' : ' + message + '\n')


def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    page = requests.get(url).text
    soup = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)

    table = soup.find_all('table')[0]
    rows = table.find_all('tr')

    for row in rows[1:]:
        cols = row.find_all('td')
        if len(cols) >= 3:
            bank_name = cols[1].text.strip()
            market_cap = cols[2].text.strip().replace(',', '')
            data_dict = {"Name": bank_name, "MC_USD_Billion": market_cap}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)
    return df


def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    # Convert Market Cap to float
    df["MC_USD_Billion"] = df["MC_USD_Billion"].astype(float)

    # Load exchange rates from CSV
    response = requests.get(csv_path, verify=False)
    response.raise_for_status()
    exchange_rates_df = pd.read_csv(StringIO(response.text))
    exchange_rates = {row['Currency']: row['Rate'] for index, row in exchange_rates_df.iterrows()}

    # Add currency columns
    df["MC_EUR_Billion"] = df["MC_USD_Billion"] * exchange_rates["EUR"]
    df["MC_GBP_Billion"] = df["MC_USD_Billion"] * exchange_rates["GBP"]
    df["MC_INR_Billion"] = df["MC_USD_Billion"] * exchange_rates["INR"]

    df = df.round(2)
    return df


def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path, index=False)
    print(f"Data successfully saved to {output_path}")


def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)


def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print("Executing SQL Query:")
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

# --------------------------------------------------
# Here, you define the required entities and call the relevant
# functions in the correct order to complete the project.
# Note that this portion is not inside any function.
# --------------------------------------------------

# Constants
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
exchange_rate_url = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv'
db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_path = '/home/project/Largest_banks_data.csv'
table_attribs = ["Name", "MC_USD_Billion"]

# Logging and ETL steps
log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)
log_progress('Data extraction complete. Proceeding to transformation')

df = transform(df, exchange_rate_url)
log_progress('Data transformation complete. Proceeding to CSV export')

load_to_csv(df, csv_path)
log_progress('Data successfully saved to CSV')

sql_connection = sqlite3.connect(db_name)
log_progress('SQL connection established. Loading data into database')

load_to_db(df, sql_connection, table_name)
log_progress('Data loaded into database')

query_statement = f"SELECT * FROM {table_name} WHERE MC_USD_Billion >= 100"
run_query(query_statement, sql_connection)
log_progress('SQL query executed successfully. ETL process complete')

sql_connection.close()
