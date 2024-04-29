import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs, urlencode

#search a single page
def google_search(query, retries=3):
    tries = 0
    #runtime loop for the scrape
    while tries <= retries:
        try:
            url = f"https://www.google.com/search?q={query}"
            response = requests.get(url)
            results = []
            last_link = ""
            soup = BeautifulSoup(response.text, 'html.parser')
            index = 0
            for result in soup.find_all('div'):
                title = result.find('h3')
                if title:
                    title = title.text
                else:
                    continue
                base_url = ""
                link = result.find('a', href=True)
                if link:
                    link = link['href']
                    parsed_url = urlparse(link)
                    base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"                
                else:
                    continue            
                #this is the full site info we wish to extract
                site_info = {'title': title, "base_url": base_url, 'link': link, "result_number": index}
                if last_link != site_info["link"]:
                    results.append(result)
            #return our list of results
            print(f"Finished scrape with {tries} retries")
            return results
        except:
            print("Failed to scrape the page")
            print("Retries left:", retries-tries)
            tries += 1
    #if this line executes, the scrape has failed
    raise Exception(f"Max retries exceeded: {retries}")


if __name__ == "__main__":

    MAX_RETRIES = 5
    QUERIES = ["cool stuff"]
    
    for query in QUERIES:
        results = google_search("cool stuff", retries=MAX_RETRIES)
        for result in results:
            print(result)