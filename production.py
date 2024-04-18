import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs, urlencode
import csv
import concurrent
from concurrent.futures import ThreadPoolExecutor
import os
import logging
import time
from dataclasses import dataclass, field, fields, asdict
#our default user agent
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.3'}
proxy_url = "https://proxy.scrapeops.io/v1/"
API_KEY = "YOUR-SUPER-SECRET-API-KEY"

## Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class SearchData:
    name: str
    base_url: str
    link: str
    page: int
    result_number: int

    def __post_init__(self):
        self.check_string_fields()
    def check_string_fields(self):
        for field in fields(self):
            if isinstance(getattr(self, field.name), str):
                if getattr(self, field.name) == "":
                    setattr(self, field.name, f"No {field.name}")
                    continue
                value = getattr(self, field.name)
                setattr(self, field.name, value.strip())


class DataPipeline:
    def __init__(self, csv_filename="", storage_queue_limit=50):
        self.names_seen = []
        self.storage_queue = []
        self.storage_queue_limit = storage_queue_limit
        self.csv_filename = csv_filename
        self.csv_open = False
    def save_to_csv(self):
        self.csv_open = True
        data_to_save = []
        data_to_save.extend(self.storage_queue)
        self.storage_queue.clear()
        if not data_to_save:
            return
        
        keys = [field.name for field in fields(data_to_save[0])]

        file_exists = os.path.isfile(self.csv_filename) and os.path.getsize(self.csv_filename)

        with open(self.csv_filename, mode="a", encoding="UTF-8") as output_file:
            writer = csv.DictWriter(output_file, fieldnames=keys)
            if not file_exists:
                writer.writeheader()
            for item in data_to_save:
                writer.writerow(asdict(item))
        self.csv_open = False

    def is_duplicate(self, input_data):
        if input_data.name in self.names_seen:
            logger.warn(f"Duplicate Item Found: {input_data.name}. Item dropped")
            return True
        self.names_seen.append(input_data.name)
        return False

    def add_data(self, scraped_data):
        if self.is_duplicate(scraped_data) == False:
            self.storage_queue.append(scraped_data)
        if len(self.storage_queue) >= self.storage_queue_limit and self.csv_open == False:
            self.save_to_csv()

    def close_pipeline(self):
        if self.csv_open:
            time.sleep(3)
        if len(self.storage_queue) > 0:
            self.save_to_csv()

def get_scrapeops_url(url):
    payload = {'api_key': API_KEY, 'url': url, 'country': 'us'}
    proxy_url = 'https://proxy.scrapeops.io/v1/?' + urlencode(payload)
    return proxy_url

#function to search a specific page
def search_page(query, page, location="United States", headers=headers, pipeline=None):
    #url of the page we want to scrape
    url = f"https://www.google.com/search?q={query}&start={page * 10}&geo_location={location}"
    payload = {
        "api_key": API_KEY,
        "url": url,
    }
    #get the scrapeops url response
    response = requests.get(get_scrapeops_url(url))
    #soup instance for parsing
    soup = BeautifulSoup(response.text, 'html.parser')
    #find all the divs
    divs = soup.find_all("div")
    #start our index at zero, this is the result number on each page
    index = 0
    last_link = ""
    for div in divs:
        #find all h3s, the original parser broke when doing this with a proxy
        h3s = div.find_all("h3")
        if len(h3s) > 0:
            #find the link element
            link = div.find("a", href=True)
            #parse the url
            parsed_url = urlparse(link["href"])
            #reconstruct the base url
            base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
            #site_info dict object
            site_info = {'title': h3s[0].text, "base_url": base_url, 'link': link["href"], "page": page, "result_number": index}
            search_data = SearchData(
                name = site_info["title"],
                base_url = site_info["base_url"],
                link = site_info["link"],
                page = site_info["page"],
                result_number = site_info["result_number"]
            )            
            #if our link is different process the result
            if site_info["link"] != last_link:
                index += 1
                last_link = site_info["link"]
                if pipeline:
                    pipeline.add_data(search_data)

if __name__ == "__main__":
    logger.info("Log Starting")

    keywords = ["cool stuff"]


    for keyword in keywords: 
        filename = keyword.replace(" ", "-")   
        data_pipeline = DataPipeline(csv_filename=f"{filename}.csv")
        try:
            with ThreadPoolExecutor(max_workers=5) as executor:
                tasks = {executor.submit(search_page, keywords[0], page, "United States", headers, data_pipeline): page for page in range(3)}
                for future in concurrent.futures.as_completed(tasks):
                    future.result()
        finally:
            data_pipeline.close_pipeline()
            logger.info("Scrape Complete")
