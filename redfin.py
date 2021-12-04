#!/home/markd/anaconda3/bin/python3

import requests
import re
import urllib.request
import db_gateway
import email_gateway
import random
import pandas as pd
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
from time import sleep
from tenacity import retry, stop_after_attempt, wait_fixed
from config.location import *
from config.base import *


base_url = "https://www.redfin.com"

def getProxies():
    proxies = []
    proxy_req = Request('https://www.sslproxies.org')
    proxy_req.add_header('User-Agent', random.choice(user_agent_pool))
    proxy_doc = urlopen(proxy_req).read().decode('utf8')

    soup = BeautifulSoup(proxy_doc, 'html.parser')
    
    proxies_table = soup.find(lambda tag: tag.name=="table")
    rows = proxies_table.findAll(lambda tag: tag.name=="tr")
    # Save proxies in the array
    for row in rows[1:]:
        data = row.findAll(lambda tag: tag.name=="td")
        country = data[2].text
        if country == "US":
            ip = data[0].text
            port = data[1].text
            proxies.append(f"http://{ip}:{port}")
    return proxies


@retry(stop=stop_after_attempt(10), wait=wait_fixed(5))
def scrape(Location, filters, proxy_list):
    proxy = random.choice(proxy_list)
    city_path = f"/city/{Location.id}/{Location.state}/{Location.city}"
    url = base_url + city_path + "/filter/" + ",".join(filters)
    user_agent = {'User-agent': random.choice(user_agent_pool)}
    print(f"searching {url}")
    r = requests.get(url, headers=user_agent, proxies={"https":proxy})
    print(r.status_code)
    if r.status_code == 403:
        raise Exception
    return r.text

def parseDownloadLink(html, download_id="download-and-save"):
    soup = BeautifulSoup(html, 'html.parser')
    try:
        link = soup.find(id=download_id)
        href = link.get("href")
    except:
        print("Unable to download")
        return False
    print(f"Got href: {href}")
    return href

@retry(stop=stop_after_attempt(4), wait=wait_fixed(5))
def downloadCSV(url, filename, proxy_list):
    print(f"attempting download from {url}")
    proxy = urllib.request.ProxyHandler({'https': random.choice(proxy_list)})
    opener=urllib.request.build_opener(proxy)
    opener.addheaders=[('User-Agent',random.choice(user_agent_pool))]
    urllib.request.install_opener(opener)
    urllib.request.urlretrieve(url,filename)

def getListOfCities():
    pattern = re.compile("^class ([A-Za-z_]+).+")
    with open("config/location.py") as fObj:
        lines = fObj.readlines()
    
    cities = []

    for line in lines:
        matchObj = pattern.match(line)
        if matchObj:
            cities.append(matchObj.group(1))
    return cities


if __name__ == "__main__":
    unscrapable_cities = set([])
    loaded_cities = set([])
    downloaded_cities = set([])
    cities = getListOfCities()
    proxy_pool = getProxies()
    for city in cities:
        try:
            exec("html = scrape(%s, filters, proxy_pool)"%(city))
            href = parseDownloadLink(html)
            if href:
                try:
                    exec("city_name = %s.city"%(city))
                    exec("state = %s.state"%(city))
                    filename = f"{city_name}_{state}.csv"
                    downloadCSV(base_url + href, filename, proxy_pool)
                    sleep(10)
                    df = pd.read_csv(filename)
                    downloaded_cities.add(city)
                    df = db_gateway.transform(df)
                    db_gateway.load(df)
                    loaded_cities.add(city)
                except Exception as e:
                    print("Error:")
                    print(str(e))
                local_files = os.listdir(os.getcwd())
                if filename in local_files:
                    os.remove(filename)
            else:
                print(f"Unable to download from {city}")
                unscrapable_cities.add(city)
        except:
            print("Retry timeout")
        sleep(random.randint(min_delay_seconds, max_delay_seconds))
    
    successful = "\n".join(list(loaded_cities))
    unscraped = "\n".join(list(unscrapable_cities))
    error_with_download = "\n".join(list(set(cities).difference(unscrapable_cities).difference(downloaded_cities)))
    error_with_loading = "\n".join(list(downloaded_cities.difference(loaded_cities)))

    notification = """
    ----------------------------
    Cities not scraped
    ----------------------------
    %s
    ----------------------------
    Cities with download failure
    ----------------------------
    %s
    ----------------------------
    Cities with loading failure
    ----------------------------
    %s
    ----------------------------
    Cities successful
    ----------------------------
    %s
    """%(unscraped, error_with_download, error_with_loading, successful)
    print(notification)
    result = email_gateway.sendEmail(notification)
    if result:
        print(result)
    else:
        print("Notification successful.")
        


