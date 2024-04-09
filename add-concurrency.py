import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs
import csv
from concurrent.futures import ThreadPoolExecutor
#our default user agent
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.3'}
#function to search a specific page
def search_page(query, page, location="United States", headers=headers):
    #list to hold our results
    results = []
    #last_link
    last_link = ""
    #url to scrape
    url = f"https://www.google.com/search?q={query}&start={page * 10}&geo_location={location}"
    #get the response
    response = requests.get(url, headers=headers)
    #parse the html
    soup = BeautifulSoup(response.text, 'html.parser')
    #start at zero
    index = 0
    #find all the div elements on the page
    for result in soup.find_all('div'):
        #extract the title
        title = result.find('h3')
        #if we have a title, save the title
        if title:
            title = title.text
        else:
            continue
        #initiate a base_url variable
        base_url = ""
        #pull the raw link from the result
        link = result.find('a', href=True)
        #if there is a link present
        if link:
            #save the link
            link = link['href']
            #parse the url
            parsed_url = urlparse(link)
            #format the base url from the parsed url
            base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"                
        else:
            continue            
        #this is the full site info we wish to extract
        site_info = {'title': title, "base_url": base_url, 'link': link, "page": page, "result_number": index}
        #if the link is different from the last link
        if last_link != site_info["link"]:
            #save the result
            results.append(site_info)
            #increment our index
            index += 1
        #reassign the last link so we can compare it to the next one
        last_link = link
    #return our list of results
    return results
#perform a full search by searching multiple pages concurrently
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
#run the main function
search_results = full_search("cool stuff")
#path to our csv file
path_to_csv = "concurrent-search.csv"
#open the file in write mode
with open(path_to_csv, mode="w", newline="", encoding="UTF-8") as file:
    #use DictWriter to name the headers after our keys
    writer = csv.DictWriter(file, fieldnames=search_results[0].keys())
    #write the headers
    writer.writeheader()
    #write each result object as a row
    writer.writerows(search_results)