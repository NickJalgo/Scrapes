# selenium modules
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.proxy import Proxy, ProxyType
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from fake_useragent import UserAgent

# utilities
import pandas as pd
import numpy as np
import time
from time import sleep
from random import randint

# set global variables
smtprx = "gate.smartproxy.com:7000"
headless = False
wbf, sbp, mbp = (60, 10, 1)

# company and scrape specific objects
df = pd.DataFrame(columns = ["count", "ticker", "company", "title"])

cos = { "GDOT": {"company": "Green Dot", "url": "https://greendotcorp.wd1.myworkdayjobs.com/en-us/gdc", "run_scrape": True},
        "SYF": {"company": "Synchrony Financial", "url": "https://www.synchronycareers.com/ListJobs", "run_scrape": False},
        "SCHW": {"company": "Charles Schwab", "url": "https://jobs.schwabjobs.com/search-jobs", "run_scrape": False},
        "V": {"company": "Visa", "url": "https://usa.visa.com/careers.html", "run_scrape": False},
        "COF": {"company": "Capital One", "url": "https://www.capitalonecareers.com/search-jobs", "run_scrape": False},
        "ALLY": {"company": "Ally Financial", "url": "https://recruiting.adp.com/srccar/public/RTI.home?d=AllyCareers&c=1125607", "run_scrape": False},
        "FLT": {"company": "FleetCor Technologies", "url": "https://us59.dayforcehcm.com/CandidatePortal/en-US/fleetcor?=1", "run_scrape": False},
        "DFS": {"company": "Discover Financial", "url": "https://jobs.discover.com/job-search/", "run_scrape": False},
    }

# instantiate Chrome capabilties container and Options object
capabilities = webdriver.DesiredCapabilities.CHROME
chrome_options = Options()

# enable proxy rotation through SmartProxy (selenium based scrapes require whitelisting IPs)
pxy = Proxy()
pxy.proxy_type = ProxyType.MANUAL
pxy.http_proxy = smtprx
pxy.https_proxy = smtprx
pxy.ssl_proxy = smtprx
pxy.add_to_capabilities(capabilities)

# configure Chrome webdriver option
if headless: chrome_options.add_argument("--headless")
ua = UserAgent()
userAgent = ua.random
chrome_options.add_argument(f"user-agent={userAgent}")
chrome_options.add_argument("--ignore-certificate-errors")

# instantiate the driver
driver = webdriver.Chrome("chromedriver.exe", desired_capabilities=capabilities, options=chrome_options)
driver.set_window_size(3440, 1440)
driver.set_page_load_timeout(wbf)

def scrape_gdot(ticker, company, url, wbf, sbp, mbp, df):
    job_num = 1

    print(f"Starting the {ticker} - {company} scrape...")

    xp = {"b1": "//button[@data-automation-id='legalNoticeAcceptButton']",
          "jg": "//div[contains(@id, 'promptOption-gwt-uid')]",}
        
    driver.get(url)
    WebDriverWait(driver, wbf).until(EC.presence_of_all_elements_located((By.XPATH, xp["b1"])))

    try:
        driver.find_element_by_xpath(xp["b1"]).click()
        WebDriverWait(driver, wbf).until(EC.presence_of_all_elements_located((By.XPATH, xp["jg"])))
    except:
        pass

    for job in driver.find_elements_by_xpath(xp["jg"]):
        if job.text != "Privacy Statement":
            df = df.append({"count":job_num, "ticker":ticker, "company":company, "title":job.text}, ignore_index=True)
            print({"count":job_num, "ticker":ticker, "company":company, "title":job.text})
            job_num += 1
    
    print(f"...Finished with the {ticker} - {company} scrape")

    return df

def scrape_syf(ticker, company, url, wbf, sbp, mbp, df):
    job_num = 1

    print(f"Starting the {ticker} - {company} scrape...")

    xp = {"b1": "//a[contains(@class, 'country-modal')]",
          "b2": "//*[@id='dnn_ctr27660_HtmlModule_lblContent']/div/p[1]/strong",
          "b3": "",
          "tp": "",
          "jg": "//*[@id='jobGrid0']/div[2]/table",
          "js": "//*[@id='jobGrid0']/div[2]/table/tbody/tr[{}]/td[1]/a",
          "np": "//*[@id='jobGrid0']/div[3]/a[3]",
          "cn": "//p[@class='job-search__result-count small']"}

    driver.get(url)

    WebDriverWait(driver, wbf).until(EC.presence_of_all_elements_located((By.XPATH, xp["b1"])))
    driver.find_element_by_xpath(xp["b1"]).click()

    total_jobs = int(driver.find_element_by_xpath(xp["b2"]).text)
    total_pages = int(total_jobs/10) + 1

    for page in range(total_pages):
        WebDriverWait(driver, wbf).until(EC.presence_of_all_elements_located((By.XPATH, xp["jg"])))
        for i in range(10):
            try:
                job = driver.find_elements_by_xpath(xp["js"].format(i+1))[0].text
            except:
                break
            df = df.append({"count":job_num, "ticker":ticker, "company":company, "title":job}, ignore_index=True)
            print({"count":job_num, "ticker":ticker, "company":company, "title":job})
            job_num += 1
            if job_num == (total_jobs + 1):
                break
        if job_num == (total_jobs + 1):
            break

        current_page = driver.find_element_by_xpath(xp["cn"]).text
        print(current_page)

        try:
            driver.find_element_by_xpath(xp["np"]).click()
        except:
            break

        try:
            check_next_page = True
            while check_next_page:
                try:
                    next_page = driver.find_element_by_xpath(xp["cn"]).text
                    print(next_page)
                    if next_page != current_page: check_next_page = False
                    time.sleep(mbp)
                except:
                    time.sleep(mbp)
        except:
            break

    print(f"...Finished with the {ticker} - {company} scrape")

    return df

def scrape_schw(ticker, company, url, wbf, sbp, mbp, df):
    job_num = 1

    print(f"Starting the {ticker} - {company} scrape...")

    xp = {"b1": "//button[@id='gdpr-button']",
          "b2": "//*[@id='search-results']/h1",
          "b3": "",
          "tp": "",
          "jg": "//*[@id='search-results-list']/ul/li[1]/a/h2",
          "js": "//*[@id='search-results-list']/ul/li[{}]/a/h2",
          "np": "//*[@id='pagination-bottom']/div[2]/a[2]",
          "cn": "//input[@class='pagination-current']"}
    
    driver.get(url)
    sleep(20)
    print("got it")
    
    WebDriverWait(driver, wbf).until(EC.presence_of_all_elements_located((By.XPATH, xp["b1"])))
    driver.find_element_by_xpath(xp["b1"]).click()

    total_jobs_string = driver.find_element_by_xpath(xp["b2"]).text
    total_jobs = [int(s) for s in total_jobs_string.split() if s.isdigit()][0]
    total_pages = int(total_jobs/15) + 1

    for page in range(1, total_pages + 1):
        for i in range(15):
            job = driver.find_elements_by_xpath(xp["js"].format(i+1))[0].text
            df = df.append({"count":job_num, "ticker":ticker, "company":company, "title":job}, ignore_index=True)
            print({"count":job_num, "ticker":ticker, "company":company, "title":job})
            job_num += 1
            if job_num == (total_jobs + 1): break
        if job_num == (total_jobs + 1): break
        
        current_page = driver.find_element_by_xpath(xp["cn"]).get_attribute("value")
        print(current_page)

        try:
            driver.find_element_by_xpath(xp["np"]).click()
        except:
            break
        
        try:
            check_next_page = True
            while check_next_page:
                try:
                    next_page = driver.find_element_by_xpath(xp["cn"]).get_attribute("value")
                    print(next_page)
                    if next_page != current_page: check_next_page = False
                    sleep(mbp)
                except:
                    sleep(mbp)
        except:
            break

    print(f"...Finished with the {ticker} - {company} scrape")

    return df

def scrape_v(ticker, company, url, wbf, sbp, mbp, df):
    job_num = 1

    print(f"Starting the {ticker} - {company} scrape...")

    xp = {"b1": "//*[@id='CookieReportsBanner']/div[1]/div[3]/a",
          "b2": "//*[@id='paginationTop']/div[6]/div/div/span",
          "b3": "//*[@id='paginationTop']/div[6]/div/div/ul/li[5]/a",
          "tp": "//*[@id='paginationTop']/div[5]/span[4]",
          "jg": "//*[@id='tablebody']/tr[1]/td[1]/a",
          "js": "//*[@id='tablebody']/tr[{}]/td[1]/a",
          "np": "//*[@id='next']/span",
          "cn": "//div[@aria-label='Pagination for search results']/span[2]"}

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

    try:
        total_pages = int(driver.find_element_by_xpath(xp["tp"]).text) + 1
    except:
        pass

    current_page = driver.find_element_by_xpath(xp["cn"]).text
    print(current_page)

    for page in range(total_pages):
        WebDriverWait(driver, wbf).until(EC.presence_of_all_elements_located((By.XPATH, xp["jg"])))
        for i in range(50):
            try:
                job = driver.find_elements_by_xpath(xp["js"].format(job_num))[0].text
                df = df.append({"count":job_num, "ticker":ticker, "company":company, "title":job}, ignore_index=True)
                print({"count":job_num, "ticker":ticker, "company":company, "title":job})
                job_num += 1
            except:
                break

        try:
            driver.find_element_by_xpath(xp["np"]).click()
        except:
            break

        try:
            check_next_page = True
            while check_next_page:
                try:
                    next_page = driver.find_element_by_xpath(xp["cn"]).text
                    print(next_page)
                    if next_page != current_page: check_next_page = False
                    sleep(mbp)
                except:
                    sleep(mbp)
        except:
            break

    print(f"...Finished with the {ticker} - {company} scrape")

    return df

def scrape_cof(ticker, company, url, wbf, sbp, mbp, df):
    job_num = 1

    print(f"Starting the {ticker} - {company} scrape...")

    xp = {"b1": "//button[@id='gdpr-button']",
          "b2": "",
          "b3": "",
          "tp": "//h1[@class='page-header']",
          "jg": "//*[starts-with(@class, 'job-link')]",
          "js": "",
          "np": "//a[@rel='nofollow'][@class='next']",
          "cn": ""}

    driver.get(url)

    try:
        WebDriverWait(driver, wbf).until(EC.presence_of_all_elements_located((By.XPATH, xp["b1"])))
        driver.find_element_by_xpath(xp["b1"]).click()
    except:
        pass

    total_jobs_string = driver.find_element_by_xpath(xp["tp"]).text
    total_jobs = [int(s) for s in total_jobs_string.split() if s.isdigit()][0]
    total_pages = int(total_jobs/15) + 2
    print(f"Total pages {total_pages - 1}")

    for page in range(1, total_pages):
        WebDriverWait(driver, wbf).until(EC.presence_of_all_elements_located((By.XPATH, xp["jg"])))
        current_page = driver.find_element_by_class_name("pagination-current").get_attribute("value")
        print(f"Current page polled value: {current_page}")

        try:
            for job in driver.find_elements_by_xpath(xp["jg"]):
                title = job.find_element_by_tag_name("h2").text
                df = df.append({"count":job_num, "ticker":ticker, "company":company, "title":title}, ignore_index=True)
                print({"count":job_num, "ticker":ticker, "company":company, "title":title})
                job_num += 1
        except:
            break

        if job_num != total_jobs + 1:
            WebDriverWait(driver, wbf).until(EC.presence_of_all_elements_located((By.XPATH, xp["np"])))
        
        try:
            driver.find_element_by_xpath(xp["np"]).click()
            check_next_page = True
            cycles = 1
            while check_next_page & (cycles <= 10):
                try:
                    next_page = driver.find_element_by_class_name("pagination-current").get_attribute("value")
                    print(f"Next page polled value: {next_page} after {cycles} cycles")
                    if (next_page != current_page): check_next_page = False
                    sleep(mbp + randint(10, 15))
                    cycles += 1
                except:
                    sleep(mbp + randint(10, 15))
                    cycles += 1
        except:
            break

    return df

def scrape_ally(ticker, company, url, wbf, sbp, mbp, df):
    job_num, check_site, t, retries, start = (1, True, 0, 1, time.time())

    print(f"Starting the {ticker} - {company} scrape...")

    xp = {"b1": "",
          "b2": "",
          "b3": "",
          "tj": "//span[@class='currentCount']",
          "tp": "",
          "jg": "//*[starts-with(@class, 'jobtitle')]",
          "js": "",
          "np": "//*[starts-with(@ng-click, 'doNextPage()')]",
          "cn": "//input[@id='page_']"}

    driver = retry_url(url, 6)
    if driver == "failed": return df

    WebDriverWait(driver, wbf).until(EC.presence_of_all_elements_located((By.XPATH, xp["tj"])))

    tag_test = retry_tag(driver, xp["tj"], mbp, 6)
    if tag_test == "passed":
        total_jobs = int(driver.find_element_by_xpath(xp["tj"]).text)
    else:
        print(f"{ticker} could not retrieve total jobs. Scrape failed to retrieve any data...")
        return df

    page_adjustment = 0 if total_jobs % 10 == 0 else 1
    total_pages = int(total_jobs / 10) + page_adjustment
    print(f"{ticker} has {total_jobs} total jobs distributed over {total_pages} pages...")

    for i in range(1, total_pages + 1):
        try:
            current_page = driver.find_element_by_xpath(xp["cn"]).get_attribute("value")
            print(f"Current page marker -> {current_page}")
            WebDriverWait(driver, wbf).until(EC.presence_of_all_elements_located((By.XPATH, xp["jg"])))
            for job in driver.find_elements_by_xpath(xp["jg"]):
                df = df.append({"count":job_num, "ticker":ticker, "company":company, "title":job.text}, ignore_index=True)
                print({"count":job_num, "ticker":ticker, "company":company, "title":job.text})
                job_num += 1
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
                    print(f"Next page marker -> {next_page} after {cycles} cycles")
                    if (next_page != current_page): check_next_page = False
                    sleep(mbp)
                    cycles += 1
                except:
                    sleep(mbp)
                    cycles += 1
        except:
            break

    return df

ticker = "GDOT"
if cos[ticker]["run_scrape"]:
    try:
        df = scrape_gdot(ticker, cos[ticker]["company"], cos[ticker]["url"], wbf, sbp, mbp, df)
    except:
        pass

ticker = "SYF"
if cos[ticker]["run_scrape"]:
    try:
        df = scrape_syf(ticker, cos[ticker]["company"], cos[ticker]["url"], wbf, sbp, mbp, df)
    except:
        pass

ticker = "SCHW"
if cos[ticker]["run_scrape"]:
    try:
        df = scrape_schw(ticker, cos[ticker]["company"], cos[ticker]["url"], wbf, sbp, mbp, df)
    except:
        pass

ticker = "V"
if cos[ticker]["run_scrape"]:
    try:
        df = scrape_v(ticker, cos[ticker]["company"], cos[ticker]["url"], wbf, sbp, mbp, df) 
    except:
        pass

ticker = "FLT"
if cos[ticker]["run_scrape"]:
    try:
        df = scrape_flt(ticker, cos[ticker]["company"], cos[ticker]["url"], wbf, sbp, mbp, df)
    except:
        pass

ticker = "COF"
if cos[ticker]["run_scrape"]:
    try:
        df = scrape_cof(ticker, cos[ticker]["company"], cos[ticker]["url"], wbf, sbp, mbp, df)
    except:
        pass

ticker = "ALLY"
if cos[ticker]["run_scrape"]:
    try:
        df = scrape_ally(ticker, cos[ticker]["company"], cos[ticker]["url"], wbf, sbp, mbp, df)
    except:
        pass

try:
    driver.close()
    driver.quit()
except:
    pass

if df.shape[0] > 0:
    local_time = time.strftime("%a-%d-%b-%y_%H-%M-%S", time.localtime())
    df.to_excel(r"C:\Scrapes\Jobs\Tests\job_postings_selenium_" + local_time + ".xlsx", index=False)