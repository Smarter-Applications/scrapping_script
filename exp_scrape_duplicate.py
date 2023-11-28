import asyncio
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import pandas as pd
import re
import time
import requests
chrome_options = Options()
chrome_options.add_argument("--headless")  # Run in background


def get_brand_list_from_page(url):
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Run in background
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)       
    driver.get(url)

    html = driver.page_source
    # Parse the HTML content using BeautifulSoup
    soup = BeautifulSoup(html, 'html.parser')

    # Find the 'brands-wrap' section and then all links within it
    brands_wrap = soup.find(class_="brands-wrap")
    brands_links = brands_wrap.find_all('a') if brands_wrap else []

    # Extract link href and text
    brands_data = [{'Name': link.get_text(strip=True), 'Link': link.get('href')} for link in brands_links]

    # Create a DataFrame
    df_brands = pd.DataFrame(brands_data)

    # Append the base URL to each link
    base_url = "https://www.nutritionix.com"
    df_brands['Link'] = df_brands['Link'].apply(lambda link: base_url + link)
    return df_brands

def extract_table_to_df(table):
    rows = table.find_all('tr')
    headers = [header.get_text(strip=True) for header in rows[0].find_all('th')]
    data = [[ele.get_text(strip=True) for ele in row.find_all(['td', 'th'])] for row in rows[1:]]
    temp=pd.DataFrame(data, columns=headers)
    print(len(temp))
    return temp

# Extract URLs and Serving Sizes
def extract_urls_and_serving_sizes(table, base_url):
    urls, serving_sizes = [], []
    for row in table.find_all('tr')[1:]:
        link = row.find('a', href=True)
        urls.append(base_url + link['href'] if link else None)
        serving_size_tag = row.find('span', class_='grey')
        serving_sizes.append(serving_size_tag.get_text(strip=True) if serving_size_tag else None)
    return urls, serving_sizes

def get_last_tag(url):
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Run in background
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.get(url)
    # Wait for JavaScript to load
    driver.implicitly_wait(10)

    # Get the HTML of the page
    html = driver.page_source

    # Don't forget to close the driver
    driver.quit()

    soup = BeautifulSoup(html, 'html.parser')
    showing_tag = soup.find(lambda tag: tag.name and tag.get("class") == ["text-center", "ng-binding"] and "Showing" in tag.text)
    # Extracting the last number from the found tag
    if showing_tag:
        showing_tag_text = showing_tag.get_text()
        last_number_in_showing_tag = re.search(r'\d+$', showing_tag_text)
        last_number = int(last_number_in_showing_tag.group()) if last_number_in_showing_tag else None
    else:
        last_number = None
    return showing_tag, last_number
async def get_product_data(count,product_url,brand_url,last_number,error_count):
    all_dfs=[]
    i=1
    timedelay=0
    while i<1000000:
        try:
            # Set up the driver
            chrome_options = Options()
            chrome_options.add_argument("--headless")  # Run in background
            driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
            url =product_url+ f"?page={i}"
            driver.get(url)

            # Wait for JavaScript to load
            driver.implicitly_wait(10)

            # Get the HTML of the page
            html = driver.page_source

            # Don't forget to close the driver
            driver.quit()

            soup = BeautifulSoup(html, 'html.parser')

            # Find all tables on the page
            tables = soup.find_all('table')

            # Base URL for constructing full URLs
            base_url = 'https://www.nutritionix.com'

            # Process the table of interest
            
            if tables:
                if len(tables)==1:
                    table_of_interest=tables[0]
                else:
                    table_of_interest = tables[1]
            time.sleep(timedelay)
            df = extract_table_to_df(table_of_interest)
            time.sleep(timedelay)
            urls, serving_sizes = extract_urls_and_serving_sizes(table_of_interest, base_url)
            time.sleep(timedelay)
            df['URL'] = urls
            df['Serving Size'] = serving_sizes
            df=df.reset_index(drop=True)
            count+=len(df)
            all_dfs.append(df)
            print("Page no. ", i, " successful",count," ------- ",len(df),"last_number: "+str(last_number),"brand_url: "+str(brand_url),url)
            timedelay=0
        except Exception as e:
            print("Page no. ", i, " unsuccessful", "Error:", e,"last_number: "+str(last_number),"brand_url: "+str(brand_url),url)
            timedelay+=1
            if timedelay<3:
                continue
            error_count+=1
            count+=15
        if count>=last_number:
            break
        if error_count>30:
            print("some issue with code or product urls")
            break
        i+=1
    big_df=pd.DataFrame([])
    try:
        big_df = pd.concat(all_dfs,ignore_index=True)
        big_df['BRAND_NAME']=brand_url
    except:
        big_df=pd.DataFrame([])
        
    return big_df


    
    
async def main():
    all_brands_df=pd.read_csv("./intermediate_csv/102124.csv")
    file=open("./intermediate_csv/not_found.txt",'a')
    file1=open("./intermediate_csv/not_found_brands.txt",'a')
    base_brand_url_all="https://www.nutritionix.com/brands/grocery"
    error_count_groc=0
    count_brands=len(all_brands_df['BRAND_NAME'].value_counts())
    savu=0
    showing_tag_brands, total_brands=get_last_tag(base_brand_url_all)
    print(count_brands,"already_presnet_brands")
    for j in range(1,100000):
        df_brands=[]
        try:
            base_brand_url=base_brand_url_all+ f"?page={j}"
            df_brands=get_brand_list_from_page(base_brand_url)
            print(count_brands,"------------------",len(df_brands))
        except Exception as eb:
            error_count_groc+=1
            print(base_brand_url," : ",str(eb))
            file1.write(base_brand_url +", "+str(eb)+ "\n")
            continue
        if error_count_groc>10000:
            print("some issue with code or brand urls")
            break
        tasks = []
        for k in range(len(df_brands)):
            count = 0
            error_count = 0
            if df_brands['Name'][k] in list(all_brands_df['BRAND_NAME']):
                print("------Already Present----",df_brands['Name'][k])
                continue
            try:
                product_url = df_brands['Link'][k]
                showing_tag, last_number = get_last_tag(product_url)
                int(str(last_number))
                print(showing_tag, last_number, product_url, last_number)
                task = get_product_data(0, product_url,df_brands['Name'][k],last_number, error_count)
                tasks.append(task)
                count_brands+=1
            except Exception as ep:
                file.write(product_url + "," + str(df_brands['Name'][k])+","+str(ep) + "\n")
                print(product_url," : ",str(ep))
                continue
        # Combine all dataframes into one big dataframe
        # Run all tasks concurrently
        if len(tasks)<=0:
            continue
        all_results = await asyncio.gather(*tasks)
        all_results=pd.concat(all_results,ignore_index=True)
        all_brands_df=all_brands_df.append(all_results.copy())
        if (len(all_brands_df)//1000)>savu:
            savu=len(all_brands_df)//1000
            all_brands_df.to_csv("intermediate_csv/"+str(len(all_brands_df))+".csv",index=False)
        print(len(all_brands_df),"-------len_total-----")
        print(count_brands,total_brands)
        if count_brands>=total_brands:
            break
    file1.close()
    file.close()
    all_brands_df.to_csv("intermediate_csv/final_data"+str(len(all_brands_df))+".csv",index=False)
    return all_brands_df

# Run the async main function
asyncio.run(main())
