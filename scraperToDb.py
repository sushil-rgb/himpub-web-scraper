import mysql.connector
import time
from bs4 import BeautifulSoup
import requests
from tools import LightningScraping, HimpubCategoryLinkScraper, Flattened, get_user_agent


start_time = time.time()

# Decrease time interval for faster scraping. However, I ever discourage you to do so as it will hurt the server and may throw an error:
interval = 3


himpub_base_url = "https://www.himpub.com"

headers = {"User-Agent": get_user_agent()}


# Making a connection to sqlite database:    
conn = mysql.connector.connect(
    host = 'localhost',
    user = 'root',
    password = '',
    database = 'himpub',
    
)

curr = conn.cursor()
table_name = """CREATE TABLE books(Name VARCHAR(50), ISBN VARCHAR(50), Student_Price VARCHAR(50), Library_Price VARCHAR(50), Edition VARCHAR(50), Year_Of_Publication VARCHAR(50), Book_links VARCHAR(50), Image_links VARCHAR(50))"""
curr.execute(table_name)

# scrape all category links
category_links = HimpubCategoryLinkScraper().scrape(himpub_base_url)


book_links = []  # This list will store all category book links:


# Function to scrape all books links:
def scrapeBookLinks(url):
    r = requests.get(url, headers=headers)
    soup = BeautifulSoup(r.content, 'html.parser')
    links = [link.find('a').get('href') for link in soup.find_all('div', class_='productinfo text-center')]
    book_links.append(links)


# Scrape all category links concurrently and stored in book_links:
LightningScraping().scrape(scrapeBookLinks, category_links)

himpubs = []  # This list will store all data of books:


# Function to scrape all book datas:
def scrapeData(url):    
    req = requests.get(url, headers=headers)
    time.sleep(interval)
    soup = BeautifulSoup(req.content, 'html.parser')
    book_url = url
    # print(book_url)    

    try:
        name = soup.find('span', id='ctl00_ContentPlaceHolder1_lblMainTitle').text.strip()
    except AttributeError:
        name = "N/A"
    
    try:
        isbn = soup.find('span', id='ctl00_ContentPlaceHolder1_lblIsbnno').text.strip()
    except AttributeError:
        isbn = "N/A"

    try:
        student_price = soup.find('span', id='ctl00_ContentPlaceHolder1_studEdiRupee').text.strip()
    except AttributeError:
        student_price = "N/A"
    
    try:
        library_price = soup.find('span', id='ctl00_ContentPlaceHolder1_libprice').text.strip()
    except AttributeError:
        library_price = "N/A"
    
    try:
        book_edition = soup.find('span', id='ctl00_ContentPlaceHolder1_lbledition').text.strip()
    except AttributeError:
        book_edition = "N/A"

    try:
        year_of_publication = soup.find('span', id='ctl00_ContentPlaceHolder1_lblYop').text.strip()
    except AttributeError:
        year_of_publication = "N/A"

    try:
        image_url = soup.find('img', id='ctl00_ContentPlaceHolder1_imgBook').get('src')
    except:
        image_url = "N/A"
    
    # print books name:
    print(name)
    
    himpubs.append((name, isbn, student_price, library_price, book_edition, year_of_publication, book_url, image_url))
       

# Scrape all book data concurrently and stores in a himpub lists:
LightningScraping().scrape(scrapeData, Flattened().flat(book_links))


# Write scraped datas to database:
curr.executemany("INSERT INTO books VALUES(%s,%s,%s,%s,%s,%s,%s,%s)", himpubs)
conn.commit()
conn.close()

total_time = total_time = round(time.time()-start_time, 2)
time_in_secs = round(total_time, 2)
time_in_mins = round(total_time/60, 2)

print(f"{time_in_secs} seconds")
print(f"{time_in_mins} minutes.")
