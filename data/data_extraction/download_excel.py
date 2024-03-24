import warnings
warnings.filterwarnings("ignore")

import os
# os.system("pip install nest_asyncio")
# os.system("pip install -U selenium")
# os.system("pip install webdriver-manager")
import nest_asyncio
import time
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

import nest_asyncio
nest_asyncio.apply()


### WebScrapping Fundamentals Stock Data
def initialize_selenium():
    prefs = {"download.default_directory": str(os.path.join(os.getcwd(), "data", "Nifty50","CompanyInf50")),
             "download.prompt_for_download": False,
             "download.directory_upgrade": True}

    options = Options()
    options.add_argument("start-maximized")
    options.add_experimental_option("prefs", prefs)

    driver = webdriver.Chrome(service=Service(
        ChromeDriverManager().install()), options=options)
    return driver


def OpenAndLogin(driver, url):
    user_n = "bhumika.yaduwanshi@imentus.com"
    user_p = "toss.123"
    path_to_idlogin = '''//a[@class="button account"]'''
    path_to_uname = '''//input[@name="username" and @id="id_username"]'''
    path_to_upwd = '''//input[@name="password" and @id="id_password"]'''
    path_to_login = '''//button[@type="submit" and @class="button-primary"]'''
    driver.get(url)
    time.sleep(2)
    elem = driver.find_element(By.XPATH, path_to_idlogin)
    elem.click()
    time.sleep(2)
    elem = driver.find_element(By.XPATH, path_to_uname)
    elem.clear()
    elem.send_keys(user_n)
    time.sleep(2)
    elem = driver.find_element(By.XPATH, path_to_upwd)
    elem.clear()
    elem.send_keys(user_p)
    time.sleep(1)
    elem = driver.find_element(By.XPATH, path_to_login)
    elem.click()
    return True


def downloadXlsx(driver, stock_name):
    path_to_searchbar = '''//div[@class="search"]/*/input[@type="search" and @placeholder="Search for a company"]'''
    path_to_download = '''//button[@aria-label="Export to Excel"]/span[text()="Export to Excel"]'''
    time.sleep(2)
    elem = driver.find_element(By.XPATH, path_to_searchbar)
    elem.clear()
    elem.send_keys(stock_name)
    time.sleep(1)
    elem.send_keys(Keys.RETURN)
    time.sleep(4)
    elem = driver.find_element(By.XPATH, path_to_download)
    elem.click()


def PerformWebScrapping(stocklist):
    url = "http://www.screener.in/"
    driver = initialize_selenium()
    isLoggedIn = OpenAndLogin(driver, url)
    if isLoggedIn == True:
        for i in stocklist:
            downloadXlsx(driver, i)
    time.sleep(5)
    driver.close()
    return True


company_name = ['ADANIPORTS',
 'APOLLOHOSP',
 'ASIANPAINT',
 'AXISBANK',
 'BAJAJ AUTO',
 'BAJFINANCE',
 'BAJAJFINSV',
 'BPCL',
 'BHARTIARTL',
 'BRITANNIA',
 'CIPLA',
 'COALINDIA',
 'DIVISLAB',
 'DRREDDY',
 'EICHERMOT',
 'GRASIM',
 'HCLTECH',
 'HDFCBANK',
 'HDFCLIFE',
 'HEROMOTOCO',
 'HINDALCO',
 'HINDUNILVR',
 'HDFC',
 'ICICIBANK',
 'ITC',
 'INDUSINDBK',
 'INFY',
 'JSWSTEEL',
 'KOTAKBANK',
 'LT',
 'M&M',
 'MARUTI',
 'NTPC',
 'NESTLEIND',
 'ONGC',
 'POWERGRID',
 'RELIANCE',
 'SBILIFE',
 'SHREECEM',
 'SBIN',
 'SUNPHARMA',
 'TCS',
 'TATACONSUM',
 'TATAMOTORS',
 'TATASTEEL',
 'TECHM',
 'TITAN',
 'UPL',
 'ULTRACEMCO',
 'WIPRO']

SeleniumOk = PerformWebScrapping(company_name)
print(SeleniumOk)