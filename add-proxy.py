import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs, urlencode
import csv
from os import path
from concurrent.futures import ThreadPoolExecutor

#our default user agent
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.3'}
proxy_url = "https://proxy.scrapeops.io/v1/"
API_KEY = "YOUR-SUPER-SECRET-API-KEY"


def get_scrapeops_url(url, location='us'):
    payload = {'api_key': API_KEY, 'url': url, 'country': location}
    proxy_url = 'https://proxy.scrapeops.io/v1/?' + urlencode(payload)
    return proxy_url

def write_page_to_csv(filename, object_array):
    path_to_csv = filename
    file_exists = path.exists(filename)
    with open(path_to_csv, mode="a", newline="", encoding="UTF-8") as file:
        #name the headers after our object keys
        writer = csv.DictWriter(file, fieldnames=object_array[0].keys())
        if not file_exists:
            writer.writeheader()
        writer.writerows(object_array)

def search_page(query, page, location="United States", retries=3, num=100):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.3'}
    results = []
    last_link = ""
    tries = 0
    success = False
    while tries <= retries and not success:
        try:
            url = f"https://www.google.com/search?q={query}&start={page * num}&num={num}"
            response = requests.get(get_scrapeops_url(url), headers=headers)
            if response.status_code != 200:
                print("Failed server response", response.status_code)
                raise Exception("Failed server response!")
            print(f"Response Code: {response.status_code}")
            soup = BeautifulSoup(response.text, 'html.parser')
            index = 0
            for result in soup.find_all('div'):
                title = result.find('h3')
                if title:
                    title = title.text
                else:
                    continue
                base_url = ""
                #pull the raw link from the result
                link = result.find('a', href=True)
                if link:
                    link = link['href']
                    parsed_url = urlparse(link)
                    base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"                
                else:
                    continue            
                #this is the full site info we wish to extract
                site_info = {'title': title, "base_url": base_url, 'link': link, "page": page, "result_number": index}
                #if the link is different from the last link
                if last_link != site_info["link"]:
                    results.append(site_info)
                    index += 1
                    last_link = link
                    
            write_page_to_csv(f"{query}.csv", results)
            success = True

        except:
            print(f"Failed to scrape page {page}")
            print(f"Retries left: {retries-tries}")
            tries += 1
    if not success:
        print(f"Failed to scrape page {page}, no retries left")
        raise Exception(f"Max retries exceeded: {retries}")
    else:
        print(f"Scraped page {page} with {retries} retries left")


def full_search(query, pages=3, location="us", MAX_THREADS=5, MAX_RETRIES=4, num=100):
    page_numbers = list(range(pages))
    full_results = []
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:    
        executor.map(search_page, [query]*pages, page_numbers, [location]*pages, [MAX_RETRIES]*pages, [num]*pages)

if __name__ == "__main__":

    MAX_RETRIES = 5
    RESULTS_PER_PAGE = 10
    QUERIES = ["cool stuff"]
    
    for query in QUERIES:
        full_search(query, pages=3, num=RESULTS_PER_PAGE)
