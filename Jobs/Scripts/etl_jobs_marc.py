# database connectivity modules
from sqlalchemy import create_engine
import urllib
import pyodbc
from time import sleep

# General variables
jobs_db = urllib.parse.quote_plus("DRIVER={SQL Server};SERVER=EC2AMAZ-BQIRE63;DATABASE=scrapes")
jobs_engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(jobs_db))

# Database functions, variables, and connections
def open_db_cnx(db):
    if db == "scrapes":
        cnx = pyodbc.connect("Driver={SQL Server};"
                             "Server=EC2AMAZ-BQIRE63;"
                             "Database=scrapes;"
                             "Trusted_Connection=yes;")
    return cnx

def print_and_wait(message, wait_time):
    print(message)
    sleep(wait_time)
    return

# Run ETL that transfers staged data into fact table
cnx = open_db_cnx("scrapes")
cursor = cnx.cursor()
sp = """exec scrapes.etl.append_jobs_marc"""
cnx.autocommit = True
print_and_wait("Connecting to the Microsoft SQL Server database --> scrapes", 5)

cursor.execute(sp)
print_and_wait("Executed stored procedure --> scrapes.etl.append_jobs_marc", 10)

cnx.close
print_and_wait("Closing db connection and exiting process", 10)