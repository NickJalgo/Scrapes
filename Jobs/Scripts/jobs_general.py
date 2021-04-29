# selenium modules
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.proxy import Proxy, ProxyType
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from fake_useragent import UserAgent

# html modules
import requests
from bs4 import BeautifulSoup
import json

# analytics and utilities modules
import pandas as pd
import numpy as np
import sys
import datetime
import time
from time import sleep
from ast import literal_eval
from random import randint

# database connectivity modules
from sqlalchemy import create_engine
import urllib
import pyodbc

# Configuring selenium
capabilities = webdriver.DesiredCapabilities.CHROME
chrome_options = Options()
ua = UserAgent()
userAgent = ua.random
chrome_options.add_argument("--headless")
chrome_options.add_argument(f"user-agent={userAgent}")
driver = webdriver.Chrome(r"C:\Scrapes\ChromeDriver\chromedriver.exe", desired_capabilities=capabilities, options=chrome_options)
driver.set_window_size(3440, 1440)

# Configuring Zyte to work with requests
proxy_host, proxy_port, proxy_auth = ("proxy.crawlera.com", "8011", "c1e4aa27f76e417da8a7e01f8f06fe54:")
proxies = {"https": f"https://{proxy_auth}@{proxy_host}:{proxy_port}/", "http": f"http://{proxy_auth}@{proxy_host}:{proxy_port}/"}

# General variables
df = pd.DataFrame(columns = ["date", "job_count", "perf_time", "ticker", "description"])
wbf, sbf, mbf = (60, 10, 0.5)
jobs_db = urllib.parse.quote_plus("DRIVER={SQL Server};SERVER=EC2AMAZ-BQIRE63;DATABASE=scrapes")
jobs_engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(jobs_db))
scrape = True

# Command-line parameters
ticker = sys.argv[1]

# Database functions, variables, and connections
def open_db_cnx(db):
    if db == "scrapes":
        cnx = pyodbc.connect("Driver={SQL Server};"
                             "Server=EC2AMAZ-BQIRE63;"
                             "Database=scrapes;"
                             "Trusted_Connection=yes;")
    return cnx

def get_db_data(stmt, db):
    cnx = open_db_cnx(db)
    df = pd.read_sql(stmt, cnx)
    cnx.close()
    return df

def stage_jobs_data(df, ticker, engine):
    df.to_sql('jobs_marc', schema='stage', con=engine, chunksize=1, method='multi', index=False, if_exists="append")
    print(f"{ticker} scrape was successful...")
    return (df.iloc[0:0], False)

# Database queries
sql = (f"select * from scrapes.scrape.jobs_marc where ticker='{ticker}'", "scrapes")

# Get company metadata
s_meta = get_db_data(sql[0], sql[1])
ticker, url, headers, xp = (s_meta.iloc[0]["ticker"], s_meta.iloc[0]["base_url"], literal_eval(s_meta.iloc[0]["header"]), literal_eval(s_meta.iloc[0]["xpath"]))
