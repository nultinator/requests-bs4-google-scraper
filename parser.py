import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs, urlencode

#get the response
def google_search(query):
    #url to fetch
    url = f"https://www.google.com/search?q={query}"
    #get the url
    response = requests.get(url)
    #list to hold our results
    results = []
    #last link we parsed
    last_link = ""
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
        site_info = {'title': title, "base_url": base_url, 'link': link, "result_number": index}
        if last_link != site_info["link"]:
            results.append(result)
    #return our list of results
    return results

results = google_search("cool stuff")

for result in results:
    print(result)