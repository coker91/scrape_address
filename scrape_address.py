import requests
import pandas as pd
import numpy as np
import logging
import datetime
import time
import pytz
import threading, queue
#library untuk koneksi ke mysql
#import mysql.connector as mysql

from requests import get
from bs4 import BeautifulSoup
from time import sleep
from random import randint
from threading import Thread
#from queue import queue

q = queue.Queue(maxsize=0)
num_threads = 100
num_range = 5

headers = {"Accept-Language": "en-US,en;q=0.5", "User-Agent":"Mozilla/5.0"}

log = datetime.datetime.now().strftime("%d-%m-%y")
logging.basicConfig(filename='logfile-' + str(log) + '.txt', level=logging.INFO, format='%(asctime)s %(message)s')

address = []
city = []
province = []
postal_code = []
phone = []
first_name = []
last_name = []
url = []
created_at = []
updated_at = []

def scrapping(page):
    fetchUrl = "https://www.locatefamily.com/Street-Lists/Indonesia/index-"+ str(page)+ ".html"
    fetch = requests.get(fetchUrl, headers=headers)

    if fetch.status_code != 200:
        logging.error("Gagal Fetch di Index ke - " + str(page))
    else:
        soup = BeautifulSoup(fetch.text, 'html.parser')
        alamat_div = soup.find_all('li', class_='list-group-item')
        countTotalEl = len(alamat_div)

        #sleep(randint(2,3))
        for element in alamat_div:
            url.append(fetchUrl)
            created_at.append(datetime.datetime.now(tz=pytz.timezone('Asia/Jakarta')).strftime("%m/%d/%Y, %H:%M:%S"))
            updated_at.append(datetime.datetime.now(tz=pytz.timezone('Asia/Jakarta')).strftime("%m/%d/%Y, %H:%M:%S"))
            findElementStreetAddress = element.find('span', attrs={"itemprop": "streetAddress"})
            address1 = findElementStreetAddress.text if hasattr(findElementStreetAddress, 'text') else ""
            address.append(address1)

            findElementAddressLocality = element.find('span', attrs={"itemprop": "addressLocality"})
            city1 = findElementAddressLocality.text if hasattr(findElementAddressLocality, 'text') else ""
            city.append(city1)

            findElementAddressRegion = element.find('span', attrs={"itemprop": "addressRegion"})
            province1 = findElementAddressRegion.text if hasattr(findElementAddressRegion, 'text') else ""
            province.append(province1)
            
            findElementPostalCode = element.find('span', attrs={"itemprop": "postalCode"})
            postal_code1 = findElementPostalCode.text if hasattr(findElementPostalCode, 'text') else ""
            postal_code.append(postal_code1)

            findElementPhoneLink = element.find('a', attrs={"class": "phone-link"})
            phone1 = findElementPhoneLink.text if hasattr(findElementPhoneLink, 'text') else ""
            phone.append(phone1)

            findElementGivenName = element.find('span', attrs={"itemprop": "givenName"})
            first_name1 = findElementGivenName.text if hasattr(findElementGivenName, 'text') else ""
            first_name.append(first_name1)

            findElementLastName = element.find('span', attrs={"itemprop": "familyName"})
            last_name1 = findElementLastName.text if hasattr(findElementLastName, 'text') else ""
            last_name.append(last_name1)
    print("Page ke - " + str(page))
    logging.info("Berhasil melakukan scrapping pada index ke - " + str(page) + ", dengan jumlah row : " + str(countTotalEl))
    
t0 = time.time()
def startWorking(q):
  while True:
    print(q.get())
    q.task_done()

for i in range(num_threads):
    worker = Thread(target=startWorking, args=(q,))
    worker.setDaemon(True)
    worker.start()

for page in range(1, num_range):
    q.put(scrapping(page))

addresses = pd.DataFrame({
	'address' : address,
	'city'    : city,
    'province': province,
    'postal_code' : postal_code,
    'phone'	  : phone,
	'first_name' : first_name,
	'last_name' : last_name,
	'url': url,
	'created_at': created_at,
	'updated_at': updated_at
    })

print(addresses.tail())

#addresses.to_csv('scrape.csv')
t1 = time.time() - t0
conversion = datetime.timedelta(seconds= t1)
conversion_time = str(conversion)
print(f"Waktu yang diperlukan : " + conversion_time)
logging.info("Waktu yang diperlukan : " + conversion_time)

#drop table
#mycursor.execute("DROP TABLE data")

#cara membuat database
#mycursor.execute("CREATE DATABASE scrape_alamat")

#cara membuat tabel
#mycursor.execute("CREATE TABLE data (id INT(10), address VARCHAR(255), city VARCHAR(50), phone VARCHAR(15), first_name VARCHAR(50), last_name VARCHAR(50), created_at TIMESTAMP, updated_at TIMESTAMP)")

#import library engine sql
import pymysql
pymysql.install_as_MySQLdb()

#import sqlachemy

from sqlalchemy import create_engine

print("------Terakhir----------")
#create engine sql
my_conn = create_engine("mysql+mysqldb://root:pass123@localhost/address", pool_size=10, max_overflow=20)

df = pd.DataFrame(data=addresses)
tanggal = datetime.datetime.now().strftime("%d_%m_%y")
df.to_sql(con=my_conn, name='data_' + tanggal ,if_exists='replace', index=True, index_label="id", chunksize=10000)

print("--- Data berhasil di upload ---")
