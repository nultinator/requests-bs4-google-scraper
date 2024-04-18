import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs, urlencode
import csv
import concurrent
from concurrent.futures import ThreadPoolExecutor

#our default user agent
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.3'}
proxy_url = "https://proxy.scrapeops.io/v1/"
API_KEY = "YOUR-SUPER-SECRET-API-KEY"


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
    #list to hold our results
    results = []
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
            
            #if our link is different process the result
            if site_info["link"] != last_link:
                results.append(site_info)
                index += 1
                last_link = site_info["link"]
    return results

def full_search(query, pages=3, location="United States"):
    #make a list of pages
    page_numbers = list(range(pages))
    #make a list to hold our full search result
    full_results = []
    #open a max of 5 new threads
    with ThreadPoolExecutor(max_workers=5) as executor:
        #for each page, use a thread to execute the search page function
        future_results = executor.map(search_page, [query]*pages, page_numbers, [location]*pages)
        #once we have our future results, add each result to the full result list
        for page_results in future_results:
            full_results.extend(page_results)
    #return the full result list
    return full_results

if __name__ == "__main__":

    keywords = "cool stuff"

    results = full_search(keywords)

    path_to_csv = "add-proxy.csv"

    with open(path_to_csv, mode="w", newline="", encoding="UTF-8") as file:
        #use DictWriter to name the headers after our keys
        writer = csv.DictWriter(file, fieldnames=results[0].keys())
        #write the headers
        writer.writeheader()
        #write each result object as a row
        writer.writerows(results)
