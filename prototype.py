import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs

def google_search(query, pages=3, location="United States"):
    #header to appear like a normal browser
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.3'}
    #list of results
    results = []
    #last_link
    last_link = ""
    for page in range(0, pages):
        #url with parameters for the search query and result size
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
            base_url = ""
            #pull the raw link from the result
            link = result.find('a', href=True)
            #initiate a base_url variable
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
    return results
#run the main function
search_results = google_search("cool stuff")
for result in search_results:
    print(result)
