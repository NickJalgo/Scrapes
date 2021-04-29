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
driver = webdriver.Chrome(r"C:\Scrapes\Jobs\Scripts\chromedriver.exe", desired_capabilities=capabilities, options=chrome_options)
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

# General functions
def df_add(df, count, ticker, title, time):
    count += 1
    date, time = (datetime.datetime.today().strftime("%m/%d/%Y"), "%.6f" % round(time, 6))
    return (df.append({"date": date, "job_count": count, "perf_time": time, "ticker": ticker, "description": title}, ignore_index=True), count)

def try_request(headers, ticker, target_url, proxies, max_retries, sleep, config):
    retries = 1
    while retries <= max_retries:
        response = requests.request("GET", target_url, headers=headers, proxies=proxies, verify=False)
        if response.status_code == 200:
            time.sleep(sleep)
            retries = max_retries + 2
        else:
            print(f"{ticker} url request attempt {retries} failed with error code <{response.status_code}> trying again >>>")
            time.sleep(sleep)
            retries += 1

    if retries == (max_retries + 1):
        print(f"{ticker} url request failed after {retries-1} retries; {ticker} scrape FAILED...")
        return ("FAILED", 0, 0)
    else:
        if config == "soup":
            soup = BeautifulSoup(response.content, "html.parser")
            return (soup, 0, 0)
        elif config == "json_get_total_jobs_v1":
            j = json.loads(response.text)
            total_jobs = j["body"]["children"][0]["facetContainer"]["paginationCount"]["value"]
            return (total_jobs, 0, 0)
        elif config == "json_AFRM":
            j = json.loads(response.text)
            total_jobs = j["meta"]["total"]
            return (j, total_jobs, 0)
        elif config == "soup_PYPL_0":
            soup = BeautifulSoup(response.content, "html.parser")
            total_pages = int((soup.find_all("li", class_="jrp-pagination-number"))[7].a.text)
            return (soup, total_pages, 0)
        elif config == "soup_PYPL":
            return (BeautifulSoup(response.content, "html.parser"), 0, 0)
        elif config == "json_AXP_0":
            j = json.loads(response.text)
            total_pages = int(j["totalCount"]/100) + 1
            return (j, total_pages, 0)
        elif config == "json_AXP":
            return (json.loads(response.text), 0, 0)
        elif config == "json_PNC_0":
            soup = BeautifulSoup(response.text, "html.parser").prettify()
            json_target = soup[soup.find("phApp.ddo = ")+12:soup.find("; phApp.experimentData")]
            j = json.loads(json_target)
            total_jobs = j["eagerLoadRefineSearch"]["totalHits"]
            max_pages = ((total_jobs//10) + 1) * 50
            return (j, max_pages, total_jobs)
        elif config == "json_PNC":
            soup = BeautifulSoup(response.text, "html.parser").prettify()
            json_target = soup[soup.find("phApp.ddo = ")+12:soup.find("; phApp.experimentData")]
            j = json.loads(json_target)
            return (j, 0, 0)
        elif config == "json_CFG_0":
            j = json.loads(response.text)
            soup = BeautifulSoup(j["results"], "html.parser")
            total_jobs_string = soup.find_all("h1", role="status")[0].text
            total_jobs = [int(s) for s in total_jobs_string.split() if s.isdigit()][0]
            total_pages = int(total_jobs / 100) + 1
            return (j, total_pages, total_jobs)
        elif config == "json_CFG":
            j = json.loads(response.text)
            return (BeautifulSoup(j["results"], "html.parser"), 0, 0)
        else:
            return (response, 0, 0)

def get_jobs(headers, target_url, proxies, ticker, df, job_num):
    retries, max_retries = (1, 50)
    while retries <= max_retries:
        try:
            response = requests.request("GET", target_url, headers=headers, proxies=proxies, verify=False)
            time.sleep(randint(1, 3))
            j = json.loads(response.text)
            retries = max_retries + 2
        except:
            print(f"{ticker} json load attempt {retries} failed... trying again...")
            time.sleep(randint(1, 3))
            retries += 1
    for job in j["body"]["children"][0]["children"][0]["listItems"]:
        df, job_num = df_add(df, job_num, ticker, job["title"]["instances"][0]["text"], time.perf_counter()-start)
        print(job_num, " ----> ", job["title"]["instances"][0]["text"])
    return (df, job_num)

# Start the clock
start = time.perf_counter()

if ticker in ["BILL", "CDLX", "SQ", "UPST", "AFRM"]:
    try:
        while scrape:
            if ticker == "AFRM":
                r, total_jobs, job_num = try_request(headers, ticker, url.format(0), proxies, 10, 5, "json_AFRM")
            else:
                r, job_num, garbage = try_request(headers, ticker, url, proxies, 10, 5, "soup")

            if r == "FAILED":
                break

            if ticker == "BILL":
                for job in r.find_all("h5", attrs={"data-qa": "posting-name"}):
                    df, job_num = df_add(df, job_num, ticker, job.text, time.perf_counter()-start)
            elif ticker == "CDLX":
                for job in r.find_all("a", attrs={"class": "job-listing-name-link"}):
                    df, job_num = df_add(df, job_num, ticker, job.find("h3").text, time.perf_counter()-start)
            elif ticker == "SQ":
                for job in r.find_all("div", attrs={"class": "pad-vert-line border-bottom"}):
                    df, job_num = df_add(df, job_num, ticker, job.find("a", attrs={"target": "_blank"}).text, time.perf_counter()-start)
            elif ticker == "UPST":
                for job in r.find_all("span", attrs={"class": "job-title flex-1"}):
                    df, job_num = df_add(df, job_num, ticker, job.text, time.perf_counter()-start)
            elif ticker == "AFRM":
                for i in range(total_jobs):
                    df, job_num = df_add(df, job_num, ticker, r["jobs"][i]["title"], time.perf_counter()-start)

            df, scrape = stage_jobs_data(df, ticker, jobs_engine)
    except:
        print(f"{ticker} scrape FAILED...")
        pass

if ticker in  ["ADS", "WU", "WEX", "QTWO", "NCNO", "LC", "MA"]:
    try:
        while scrape:
            total_jobs, job_num, scroll_count = try_request(headers, ticker, url.format(0), proxies, 10, 5, "json_get_total_jobs_v1")
            if total_jobs == "FAILED":
                break
            
            while scroll_count <= (total_jobs//50)*50:
                df, job_num = get_jobs(headers, url.format(scroll_count), proxies, ticker, df, job_num)
                scroll_count += 50

            df, scrape = stage_jobs_data(df, ticker, jobs_engine)
    except:
        print(f"{ticker} scrape FAILED...")
        pass

if ticker == "PYPL":
    try:
        while scrape:            
            r, total_pages, job_num = try_request(headers, ticker, url.format(1), proxies, 10, 5, "soup_PYPL_0")
            if r == "FAILED":
                break

            for i in range(total_pages):
                r, job_num, garbage = try_request(headers, ticker, url.format(i+1), proxies, 10, 5, "soup_PYPL")
                job_num = i*(job_num+1)*50
                if r == "FAILED":
                    break
                for job in r.find_all("tr", class_="job-result"):
                    df, job_num = df_add(df, job_num, ticker, job.find("a", class_="primary-text-color job-result-title").text, time.perf_counter()-start)
                print(f"Page {i+1} of {ticker} job postings scraped...")

            df, scrape = stage_jobs_data(df, ticker, jobs_engine)
    except:
        print(f"{ticker} scrape FAILED...")
        pass

if ticker == "AXP":
    try:
        while scrape:
            r, total_pages, job_num = try_request(headers, ticker, url.format(1, 10), proxies, 10, 5, "json_AXP_0")
            if r == "FAILED":
                break

            for page in range(total_pages):
                r, job_num, garbage = try_request(headers, ticker, url.format(page+1, 100), proxies, 10, 5, "json_AXP")
                job_num = page*(job_num+1)*100
                if r == "FAILED":
                    break
                for job in r["jobs"]:
                    df, job_num = df_add(df, job_num, ticker, job["data"]["title"], time.perf_counter()-start)
                print(f"Page {page+1} of {ticker} job postings scraped...")

            df, scrape = stage_jobs_data(df, ticker, jobs_engine)
    except:
        print(f"{ticker} scrape FAILED...")
        pass

if ticker == "PNC":
    try:
        while scrape:
            r, max_pages, total_jobs = try_request(headers, ticker, url.format(0, 1), proxies, 10, 5, "json_PNC_0")
            job_num = 0
            if r == "FAILED":
                break
            
            for page in range(0, max_pages, 50):
                r, job_num, garbage = try_request(headers, ticker, url.format(page, 100), proxies, 10, 5, "json_PNC")
                job_num = (page//50)*(job_num+1)*50
                if r == "FAILED":
                    break
                jobs = r["eagerLoadRefineSearch"]["data"]["jobs"]
                for job in jobs:
                    df, job_num = df_add(df, job_num, ticker, job["title"], time.perf_counter()-start)
                    if job_num == total_jobs:
                        break
                if job_num == total_jobs:
                    break
                print(f"Page {page//50+1} of {ticker} job postings scraped...")

            df, scrape = stage_jobs_data(df, ticker, jobs_engine)
    except:
        print(f"{ticker} scrape FAILED...")
        pass

if ticker == "CFG":
    try:
        while scrape:
            r, total_pages, total_jobs = try_request(headers, ticker, url.format(1, 10), proxies, 10, 5, "json_CFG_0")
            job_num = 0
            if r == "FAILED":
                break

            for i in range(total_pages):
                r, job_num, garbage = try_request(headers, ticker, url.format(i+1, 100), proxies, 10, 5, "json_CFG")
                job_num = i*(job_num+1)*100
                if r == "FAILED":
                    break
                for job in r.find_all("ul")[1].find_all("li"):
                    df, job_num = df_add(df, job_num, ticker, job.find("h2").text, time.perf_counter()-start)
                print(f"Page {i+1} of {ticker} job postings scraped...")

            df, scrape = stage_jobs_data(df, ticker, jobs_engine)
    except:
        print(f"{ticker} scrape FAILED...")
        pass

if ticker == "GDOT":
    tries, max_tries, job_num = (1, 5, 0)
    try:
        while scrape or (tries > 5):
            driver.get(url)
            try:
                WebDriverWait(driver, wbf).until(EC.presence_of_all_elements_located((By.XPATH, xp["jg"])))
                for job in driver.find_elements_by_xpath(xp["jg"]):
                    if job.text != "Privacy Statement":
                        df, job_num = df_add(df, job_num, ticker, job.text, time.perf_counter()-start)
                if df.shape[0] == 0:
                    tries += 1
                else:
                    df, scrape = stage_jobs_data(df, ticker, jobs_engine)
            except:
                tries += 1
    except:
        print(f"{ticker} scrape FAILED...")
        pass

if ticker == "SYF":
    tries, max_tries, job_num = (1, 5, 0)
    try:
        while scrape or (tries > 5):
            driver.get(url)
            try:
                WebDriverWait(driver, wbf).until(EC.presence_of_all_elements_located((By.XPATH, xp["b1"])))
                driver.find_element_by_xpath(xp["b1"]).click()
            except:
                pass
            try:
                time.sleep(randint(8, 12))
                driver.find_element_by_xpath(xp["b3"]).click()
                total_jobs = int(driver.find_element_by_xpath(xp["b2"]).text)
                total_pages = int(total_jobs/10) + 1
            except:
                break

            for page in range(total_pages):           
                WebDriverWait(driver, wbf).until(EC.presence_of_all_elements_located((By.XPATH, xp["js"].format(0))))
                try:
                    time.sleep(randint(8, 12))
                    driver.find_element_by_xpath(xp["b3"]).click()
                except:
                    pass

                for i in range(10):
                    try:
                        job = driver.find_elements_by_xpath(xp["js"].format(i))[0].text
                    except:
                        break
                    df, job_num = df_add(df, job_num, ticker, job, time.perf_counter()-start)
                    print(job_num, "  ----->  ", job)
                    if job_num == total_jobs:
                        break

                if job_num == total_jobs:
                    break

                try:
                    driver.find_element_by_xpath(xp["np"]).click()
                    time.sleep(randint(8,12))
                except:
                    pass

            df, scrape = stage_jobs_data(df, ticker, jobs_engine)
    except:
        print(f"{ticker} scrape FAILED...")
        pass

if ticker == "FLT":
    try:
        while scrape:
            job_num, cycle_page, cookie_clicked = (0, True, True)
            driver.get(url)
            time.sleep(randint(8, 12))

            while cycle_page:
                try:
                    WebDriverWait(driver, sbf).until(EC.presence_of_all_elements_located((By.XPATH, xp["cn"])))
                    current_page = driver.find_element_by_xpath(xp["cn"]).get_attribute("data-page-no")
                except:
                    cycle_page = False

                if cookie_clicked:
                    try:
                        driver.find_element_by_xpath(xp["b1"]).click()
                        cookie_clicked = False
                    except:
                        pass    
                try:
                    for job in driver.find_elements_by_xpath(xp["jg"]):
                        title = job.find_element_by_tag_name("a").text
                        df, job_num = df_add(df, job_num, ticker, title, time.perf_counter()-start)
                    time.sleep(mbf+randint(1,2))
                except:
                    cycle_page = False
                if cycle_page:
                    WebDriverWait(driver, wbf).until(EC.presence_of_all_elements_located((By.XPATH, xp["np"])))
                    driver.find_element_by_xpath(xp["np"]).click()

            df, scrape = stage_jobs_data(df, ticker, jobs_engine)
    except:
        print(f"{ticker} scrape FAILED...")
        pass

if ticker == "ALLY":
    while scrape:
        driver.get(url)
        time.sleep(randint(8,12))
        
        total_jobs = int(driver.find_element_by_xpath(xp["tj"]).text)
        page_adjustment = 0 if total_jobs % 10 == 0 else 1
        total_pages, job_num = (int(total_jobs / 10) + page_adjustment, 0)

        for i in range(1, total_pages + 1):
            try:
                WebDriverWait(driver, wbf).until(EC.presence_of_all_elements_located((By.XPATH, xp["jg"])))
                for job in driver.find_elements_by_xpath(xp["jg"]):
                    df, job_num = df_add(df, job_num, ticker, job.text, time.perf_counter()-start)
                current_page = driver.find_element_by_xpath(xp["cn"]).get_attribute("value")
            except:
                break            
            try:
                if i == total_pages:
                    break
                else:
                    driver.find_element_by_xpath(xp["np"]).click()
                check_next_page = True
                cycles = 1
                while check_next_page & (cycles <= 10):
                    try:
                        next_page = driver.find_element_by_xpath(xp["cn"]).get_attribute("value")
                        print(f"Next page marker -> {next_page} versus Current page marker -> {current_page} after {cycles} cycles")
                        if (next_page != current_page): check_next_page = False
                        time.sleep(mbf+randint(2,4))
                        cycles += 1
                    except:
                        time.sleep(mbf+randint(2,4))
                        cycles += 1
            except:
                break
        df, scrape = stage_jobs_data(df, ticker, jobs_engine)

if ticker == "DFS":
    while scrape:
        driver.get(url)
        time.sleep(randint(8,12))
        WebDriverWait(driver, wbf).until(EC.presence_of_all_elements_located((By.ID, xp["b1"])))
        total_jobs, job_num = (int(driver.find_element_by_id(xp["b1"]).text), 0)
        
        for i in range(int(total_jobs/12) + 1):
            try:
                WebDriverWait(driver, wbf).until(EC.presence_of_all_elements_located((By.XPATH, xp["jg"])))
            except:
                break
            for job in driver.find_elements_by_xpath(xp["jg"]):
                df, job_num = df_add(df, job_num, ticker, job.text, time.perf_counter()-start)
            try:
                driver.find_element_by_xpath(xp["np"]).click()
            except:
                break
            try:
                check_next_page = True
                while check_next_page:
                    try:
                        next_pages = [driver.find_element_by_xpath("//div[@id='widget-jobsearch-results-pages']//a[1]").get_attribute("aria-label"),
                                        driver.find_element_by_xpath("//div[@id='widget-jobsearch-results-pages']//a[2]").get_attribute("aria-label"),
                                        driver.find_element_by_xpath("//div[@id='widget-jobsearch-results-pages']//a[3]").get_attribute("aria-label")]
                        print(f"Next page marker -> {next_pages}")
                        if next_pages:
                            check_next_page = False
                        time.sleep(mbf_randint(2,4))        
                    except:
                        time.sleep(mbf+randint(3,6))
            except:
                break
        df, scrape = stage_jobs_data(df, ticker, jobs_engine)

if ticker == "V":
    while scrape:
        driver.get(url)
        try:
            WebDriverWait(driver, wbf).until(EC.presence_of_all_elements_located((By.XPATH, xp["b1"])))
            driver.find_element_by_xpath(xp["b1"]).click()
            WebDriverWait(driver, wbf).until(EC.presence_of_all_elements_located((By.XPATH, xp["b2"])))
            driver.find_element_by_xpath(xp["b2"]).click()
            WebDriverWait(driver, wbf).until(EC.presence_of_all_elements_located((By.XPATH, xp["b3"])))
            driver.find_element_by_xpath(xp["b3"]).click()
        except:
            pass
        time.sleep(randint(5,8))
        try:
            total_pages, job_num = (int(driver.find_element_by_xpath(xp["tp"]).text) + 1, 0)
        except:
            pass

        for page in range(total_pages):
            WebDriverWait(driver, wbf).until(EC.presence_of_all_elements_located((By.XPATH, xp["jg"])))
            for i in range(50):
                try:
                    job = driver.find_elements_by_xpath(xp["js"].format(job_num+1))[0].text
                    df, job_num = df_add(df, job_num, ticker, job, time.perf_counter()-start)
                    print(job_num, " ------> ", job)
                except:
                    break

            current_page = driver.find_element_by_xpath(xp["cn"]).text
            try:
                driver.find_element_by_xpath(xp["np"]).click()
            except:
                break
            try:
                check_next_page = True
                while check_next_page:
                    try:
                        next_page = driver.find_element_by_xpath(xp["cn"]).text
                        print(f"Next page -> {next_page} versus Current page -> {current_page}")
                        if next_page != current_page:
                            check_next_page = False
                        time.sleep(mbf+randint(1,2))
                    except:
                        time.sleep(mbf+randint(1,2))
            except:
                break
        df, scrape = stage_jobs_data(df, ticker, jobs_engine)

if ticker == "SCHW":
    while scrape:
        driver.get(url)
        time.sleep(randint(8,12))
        try:
            WebDriverWait(driver, wbf).until(EC.presence_of_all_elements_located((By.XPATH, xp["b1"])))
            driver.find_element_by_xpath(xp["b1"]).click()
        except:
            pass

        total_jobs_string = driver.find_element_by_xpath(xp["b2"]).text
        total_jobs = [int(s) for s in total_jobs_string.split() if s.isdigit()][0]
        total_pages, job_num = (int(total_jobs/15) + 1, 0)

        for page in range(1, total_pages + 1):
            for i in range(15):
                job = driver.find_elements_by_xpath(xp["js"].format(i+1))[0].text
                df, job_num = df_add(df, job_num, ticker, job, time.perf_counter()-start)
                print(job_num, " ------> ", job)
                if job_num == total_jobs:
                    break
            if job_num == total_jobs:
                break

            current_page = driver.find_element_by_xpath(xp["cn"]).get_attribute("value")
            try:
                driver.find_element_by_xpath(xp["np"]).click()
            except:
                break
            try:
                check_next_page = True
                while check_next_page:
                    try:
                        next_page = driver.find_element_by_xpath(xp["cn"]).get_attribute("value")
                        print(f"Next page -> {next_page} versus Current page -> {current_page}")
                        if next_page != current_page:
                            check_next_page = False
                        time.sleep(mbf+randint(1,2))
                    except:
                        time.sleep(mbf+randint(1,2))
            except:
                break
        df, scrape = stage_jobs_data(df, ticker, jobs_engine)

if ticker == "CMA":
    while scrape:
        driver.get(url)
        time.sleep(randint(8,12))
        try:
            WebDriverWait(driver, wbf).until(EC.presence_of_all_elements_located((By.XPATH, xp["b1"])))
            driver.find_element_by_xpath(xp["b1"]).click()
        except:
            pass

        get_total_jobs = driver.find_element_by_xpath(xp["tp"]).text
        last_space = "".join(get_total_jobs).rindex(" ")
        total_jobs = int(get_total_jobs[last_space+1:len(get_total_jobs)+1])
        total_pages, job_num = (int(total_jobs / 10) + 2, 0)

        for page in range(1, total_pages):
            WebDriverWait(driver, wbf).until(EC.presence_of_all_elements_located((By.XPATH, xp["jg"])))
            
            for job in driver.find_elements_by_xpath(xp["jg"]):
                title = job.find_element_by_tag_name("a").text
                df, job_num = df_add(df, job_num, ticker, title, time.perf_counter()-start)
                print(job_num, " ------> ", title)
                if job_num == total_jobs:
                    break
            try:
                driver.find_element_by_link_text(xp["np"]).click()
            except:
                break
            try:
                check_next_page = True
                while check_next_page:
                    try:
                        next_page = driver.find_element_by_xpath(xp["cn"]).text
                        print(f"Next page marker -> {next_page}")
                        if next_page:
                            check_next_page = False
                        time.sleep(mbf+randint(2,4))
                    except:
                        time.sleep(mbf+randint(2,4))
            except:
                break
        df, scrape = stage_jobs_data(df, ticker, jobs_engine)

if ticker == "COF":
    try:
        while scrape:
            driver.get(url)
            time.sleep(randint(8,12))

            try:
                WebDriverWait(driver, wbf).until(EC.presence_of_all_elements_located((By.XPATH, xp["b1"])))
                driver.find_element_by_xpath(xp["b1"]).click()
            except:
                pass
            print("GDPR button clicked")
            total_jobs_string = driver.find_element_by_xpath(xp["tp"]).text
            total_jobs = [int(s) for s in total_jobs_string.split() if s.isdigit()][0]
            total_pages, job_num = (int(total_jobs/15) + 2, 0)
            print("total_jobs ---> ", total_jobs, " and total_pages ---> ", total_pages)
            for page in range(1, total_pages):
                WebDriverWait(driver, wbf).until(EC.presence_of_all_elements_located((By.XPATH, xp["jg"])))
                current_page = driver.find_element_by_class_name("pagination-current").get_attribute("value")
                try:
                    for job in driver.find_elements_by_xpath(xp["jg"]):
                        title = job.find_element_by_tag_name("h2").text
                        df, job_num = df_add(df, job_num, ticker, job.text, time.perf_counter()-start)
                        print(job_num, " ------> ", job.text)
                except:
                    break
                if job_num != total_jobs:
                    WebDriverWait(driver, wbf).until(EC.presence_of_all_elements_located((By.XPATH, xp["np"])))
                else:
                    break    
                try:
                    driver.find_element_by_xpath(xp["np"]).click()
                    check_next_page = True
                    cycles = 1
                    while check_next_page & (cycles <= 10):
                        try:
                            next_page = driver.find_element_by_class_name("pagination-current").get_attribute("value")
                            print(f"Next page polled value -> {next_page} after {cycles}", "cycle" if cycles==1 else "cycles")
                            if (next_page != current_page):
                                check_next_page = False
                            time.sleep(mbf+randint(1, 3))
                            cycles += 1
                        except:
                            time.sleep(mbf+randint(1, 3))
                            cycles += 1
                except:
                    break

            df, scrape = stage_jobs_data(df, ticker, jobs_engine)
    except:
        print(f"{ticker} scrape FAILED...")
        pass

# Clean-up
try:
    driver.close()
    driver.quit()
except:
    pass