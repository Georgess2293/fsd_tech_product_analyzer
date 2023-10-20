import requests
import os
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import TimeoutException,WebDriverException
from selenium.webdriver.common.action_chains import ActionChains
import praw
from lookups import brands_url,page_limit,product_url
from time import sleep
from datetime import datetime
import pandas as pd
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from textblob import TextBlob



def return_specs_df(url,driver):
    # driver = webdriver.Chrome()
    # options=Options()
    # options.add_argument('--headless')
    # driver = webdriver.Chrome(options=options)
    driver.get(url)
    product_id=url.split('-')[1].split('.')[0]
    list_df=pd.read_html(driver.page_source)
    return_df = pd.concat(list_df, ignore_index=True)
    return_df=return_df[:75]
    return_df[1] = return_df[0] + '_' + return_df[1]
    return_df=return_df.dropna(subset=1)
    return_df.reset_index()
    result=return_df.T
    result.columns=result.iloc[1]
    data_row=result.iloc[2]
    result_df=pd.DataFrame([data_row])
    try:
        name = WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="body"]/div/div[1]/div/div[1]/h1'))).text
    except:
        name=None
    result_df.insert(0,'brand',name.split(' ')[0])
    result_df.insert(1,'model',name.split(' ',1)[1])
    result_df.insert(0,'product_id',product_id)
    return result_df


def extract_reviews_from_page(url,driver):
    # driver = webdriver.Chrome()
    # options=Options()
    # options.add_argument('--headless')
    # driver = webdriver.Chrome(options=options)
    driver.get(url)
    review_elements = driver.find_elements(By.CLASS_NAME, 'user-thread')
    product_id=url.split('-reviews-')[1].split('p')[0]
    try:
        name = WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="body"]/div/div[1]/div/div[1]/h1'))).text
    except:
        name=None
    # Loop through the review elements to extract user name, rating, and date

    reviews_data = []
    for review_element in review_elements:
        user_name_element = review_element.find_element(By.XPATH, './/*[contains(@class, "uname") or contains(@class, "uname2")]')
        user_location_element = review_element.find_element(By.CLASS_NAME, 'ulocation')
        date_element = review_element.find_element(By.CLASS_NAME, 'upost')
        text_element=review_element.find_element(By.CLASS_NAME, 'uopin')
        
        user_name = user_name_element.text
        user_location = user_location_element.text
        date = date_element.text
        
        review_text = text_element.text  # Extract the entire review text if needed
        
        reviews_data.append({
            'User Name': user_name,
            'Product_id':product_id,
            'Product Name':name.split('-')[0],
            'User location': user_location,
            'Date': date,
            'Review Text': review_text
        })
    return reviews_data


def extract_all_reviews(url,driver):
    all_reviews_data = []
    page_number=1
    url_reviews=url.split('-')[0]+'-reviews-'+url.split('-')[1]
    while True:
        current=url_reviews.split('.php')[0]
        current_url=current+'p'+str(page_number)+'.php'

        reviews_on_page = extract_reviews_from_page(current_url,driver)
    
        
        # If there are no more reviews on the page, exit the loop
        if not reviews_on_page:
            break

        all_reviews_data.extend(reviews_on_page)
        page_number += 1
    all_reviews_data_df=pd.DataFrame(all_reviews_data)
    return all_reviews_data_df
    

def scrape_amazon_reviews_df(search_query):
    # Configure Selenium to use a web driver (e.g., ChromeDriver)
    driver = webdriver.Chrome()
    options=Options()
    #options.add_argument('--headless')
    driver = webdriver.Chrome(options=options)

    # Navigate to Amazon's search results page
    base_url = 'https://www.amazon.com/'
    driver.get(base_url)
    #search_box = driver.find_element(By.ID, 'twotabsearchtextbox')
    #earch_box = WebDriverWait(driver, 50).until(EC.element_to_be_clickable((By.ID, 'twotabsearchtextbox')))


    max_retries = 5
    for retry in range(max_retries):
        try:
            search_box = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, 'twotabsearchtextbox')))
            break  # Exit the loop if the element is found
        except TimeoutException:
            if retry == max_retries - 1:
                # Handle the timeout after maximum retries
                print("Element not found after maximum retries.")
                break
            else:
                print(f"Retry {retry + 1}: Element not found. Retrying...")
    search_box.send_keys(search_query)
    search_box.send_keys(Keys.RETURN)
    sleep(2)  # Adjust the sleep time as needed to allow the page to load

    # Find and extract all product links on the search results page
    product_links = driver.find_elements(By.CSS_SELECTOR, '.s-result-item a.a-link-normal.s-no-outline')
    product_urls = [link.get_attribute('href') for link in product_links]

    # Initialize a list to store reviews
    reviews_data = []

    # Loop through product links and extract reviews
    for product_url in product_urls:
        driver.get(product_url)
        sleep(2)  # Adjust the sleep time as needed to allow the page to load

        try:
            # Extract the product name
            product_name = driver.find_element(By.ID, 'productTitle').text

            # Check if the product name contains the filter string (case-insensitive)
            # if search_query.lower() in product_name.lower():
            if product_name.lower().startswith(search_query.lower()):
                # Click on the "See all reviews" link (if available)
                try:
                    see_all_reviews_link = driver.find_element(By.PARTIAL_LINK_TEXT, 'See more reviews')
                    see_all_reviews_link.click()
                    sleep(2)  # Adjust the sleep time as needed to allow the reviews page to load
                    sleep(2)
                    sort_by_dropdown = driver.find_element(By.ID, 'sort-order-dropdown')

                    # Create an ActionChains object to perform the click action
                    actions = ActionChains(driver)
                    actions.move_to_element(sort_by_dropdown).click().perform()

                    # Select "Most recent" from the sorting options
                    most_recent_option = driver.find_element(By.PARTIAL_LINK_TEXT, 'Most recent')

                    # Create a new ActionChains object to click the "Most recent" option
                    actions = ActionChains(driver)
                    actions.move_to_element(most_recent_option).click().perform()
                    sleep(2)
                    while True:
                        # Extract and store all reviews on the page
                        review_elements = driver.find_elements(By.CSS_SELECTOR, '.a-section.review .a-section.celwidget')

                        # Loop through review elements and extract user name, rating, title, date, country, and review text
                        for review_element in review_elements:
                            # Extract user name
                            user_name_element = review_element.find_element(By.CSS_SELECTOR, '.a-profile')
                            user_name = user_name_element.text

                            # Extract star rating as an integer (1 to 5 stars)
                            star_rating_element = review_element.find_elements(By.CSS_SELECTOR, '.a-icon-star .a-icon-alt')
                            star_rating = 0
                            if star_rating_element:
                                # Extract the star rating from the alt text of the star icons
                                star_rating_text = star_rating_element[0].get_attribute('innerHTML')
                                star_rating =star_rating_text.split(' ')[0]

                            # Extract review title
                            review_title_element = review_element.find_element(By.CSS_SELECTOR, '.review-title')
                            review_title = review_title_element.text

                            # Extract review date and country
                            review_date_element = review_element.find_element(By.CSS_SELECTOR, '.review-date')
                            review_date_text = review_date_element.text
                            review_date_parts = review_date_text.split('on')
                            if len(review_date_parts) == 2:
                                review_date = review_date_parts[0].strip()
                                country = review_date_parts[1].strip()
                            else:
                                review_date = review_date_text.strip()
                                country = ""

                            # Extract review text
                            review_text_element = review_element.find_element(By.CSS_SELECTOR, '.review-text-content')
                            review_text = review_text_element.text

                            # Store the extracted data in a dictionary
                            review_data = {
                                'User Name': user_name,
                                'Rating': star_rating,
                                'Title': review_title,
                                'Date': review_date,
                                'Country': country,
                                'Review Text': review_text
                            }

                            reviews_data.append(review_data)

                        next_button = driver.find_element(By.XPATH, '//*[@id="cm_cr-pagination_bar"]/ul/li[2]')
                        if 'a-disabled' in next_button.get_attribute('class'):
                            # If the "Next" button is disabled, break the loop
                            break
                        else:
                            # Click the "Next" button to navigate to the next page of reviews
                            next_button.click()
                            sleep(2)  # Adjust the sleep time as needed
                                            
                except Exception as e:
                            print(f"No 'See all reviews' link found for product: {product_name}")

        except Exception as e:
                print(f"Error processing product: {e}")

    # Close the web driver
    driver.quit()
    reviews_df=pd.DataFrame(reviews_data)

    return reviews_df

# reddit=praw.Reddit(
#     client_id="A99udy2Ex7RaoBzW5O3Gdw",
#     client_secret="jOKXzOzOe9sk-wn-i5a7c4I4zdac4w",
#     user_agent="my-tech"
# )

def extract_reddit_comments(search_query, reddit,num_results=3):
    # Initialize lists to store data
    post_titles=[]
    usernames = []
    dates = []
    comments = []

    # Search for posts related to the search query
    search_results = reddit.subreddit("all").search(search_query, limit=num_results)

    for submission in search_results:
        # Check if the submission title contains the search query
        if search_query.lower() in submission.title.lower():
            post_title = submission.title
            # Extract comments from the submission
            submission.comments.replace_more(limit=None)
            for comment in submission.comments.list():
                post_titles.append(post_title)
                usernames.append(comment.author)
                dates.append(comment.created_utc)
                comments.append(comment.body)
       # Create a Pandas DataFrame
    data = {
        'Post Title': post_titles,
        'User Name': usernames,
        'Date': [pd.to_datetime(date, unit='s') for date in dates],
        'Review Text': comments,
    }
    df = pd.DataFrame(data)

    return df
  

def return_all_specs_df(url):
    # Initialize the Chrome web driver
    driver = webdriver.Chrome()
    options = Options()
    options.add_argument('--headless')
    driver = webdriver.Chrome(options=options)

    # Navigate to the provided URL
    driver.get(url)

    # Initialize an empty DataFrame to store the results
    all_results = pd.DataFrame()

    # Find and extract the list of brands
    brand_links = driver.find_elements(By.CSS_SELECTOR, 'div.st-text a')
    brand_urls=[]
    for brand_link in brand_links[:3]:
        # Get the URL for the brand page
        brand_url = brand_link.get_attribute('href')
        brand_urls.append(brand_url)
    for brand_link in brand_urls:
        print(f"Extracting data for brand:  ({brand_urls})")

        # Navigate to the brand page
        driver.get(brand_link)

        # Find and extract the list of product links for the current brand
        product_links = driver.find_elements(By.CSS_SELECTOR, 'div.makers ul li a')
        #return product_links
        product_urls=[]
        for product_link in product_links[:3]:
            # Get the URL for the product page
            product_url = product_link.get_attribute('href')
            product_urls.append(product_url)
        for product_link in product_urls:
            print(f"Extracting data for product:({product_urls})")
            # sleep(1)

            # Extract product specifications and add them to the results DataFrame
            product_data = return_specs_df(product_link,driver)
            #all_results = all_results.append(product_data, ignore_index=True)
            all_results=pd.concat([all_results,product_data],axis=0,ignore_index=True)
    driver.quit()

    return all_results


def return_all_reviews_df(url):
    # Initialize the Chrome web driver
    driver = webdriver.Chrome()
    options = Options()
    options.add_argument('--headless')
    driver = webdriver.Chrome(options=options)

    # Navigate to the provided URL
    driver.get(url)

    # Initialize an empty DataFrame to store the results
    all_results = pd.DataFrame()

    # Find and extract the list of brands
    brand_links = driver.find_elements(By.CSS_SELECTOR, 'div.st-text a')
    brand_urls=[]
    for brand_link in brand_links[:3]:
        # Get the URL for the brand page
        brand_url = brand_link.get_attribute('href')
        brand_urls.append(brand_url)
    for brand_link in brand_urls:
        print(f"Extracting data for brand:  ({brand_urls})")

        # Navigate to the brand page
        driver.get(brand_link)

        # Find and extract the list of product links for the current brand
        product_links = driver.find_elements(By.CSS_SELECTOR, 'div.makers ul li a')
        #return product_links
        product_urls=[]
        for product_link in product_links[:3]:
            # Get the URL for the product page
            product_url = product_link.get_attribute('href')
            product_urls.append(product_url)
        for product_link in product_urls:
            print(f"Extracting data for product:({product_urls})")
            sleep(3)

            # Extract product specifications and add them to the results DataFrame
            product_data = extract_all_reviews(product_link,driver)
            #all_results = all_results.append(product_data, ignore_index=True)
            all_results=pd.concat([all_results,product_data],axis=0,ignore_index=True)
    driver.quit()

    return all_results


def return_all_reddit_df(df,reddit):
    result_df=pd.DataFrame()
    try:
        for _,row in df.iterrows():
            reviews_df=extract_reddit_comments(f"{row['brand']} {row['model']}", reddit)
            reviews_df.insert(2,'product_id',row['product_id'])
            result_df=pd.concat([result_df,reviews_df],axis=0,ignore_index=True)
    except Exception as e:
        print(e)
    finally:
        return result_df

def return_all_amazon_df(df):
    result_df=pd.DataFrame()
    for _,row in df[:1].iterrows():
        try:
            reviews_df=scrape_amazon_reviews_df(f"{row['brand']} {row['model']}")
            reviews_df.insert(2,'product_id',row['product_id'])
            result_df=pd.concat([result_df,reviews_df],axis=0,ignore_index=True)
        except:
            continue
    return result_df



def return_all_specs_update_df(url):
    # Initialize the Chrome web driver
    driver = webdriver.Chrome()
    options = Options()
    options.add_argument('--headless')
    driver = webdriver.Chrome(options=options)

    # Navigate to the provided URL
    driver.get(url)

    # Initialize an empty DataFrame to store the results
    all_results = pd.DataFrame()

    # Find and extract the list of brands
    brand_links = driver.find_elements(By.CSS_SELECTOR, 'div.st-text a')
    brand_urls=[]
    for brand_link in brand_links[:3]:
        # Get the URL for the brand page
        brand_url = brand_link.get_attribute('href')
        brand_urls.append(brand_url)
    for brand_link in brand_urls:
        print(f"Extracting data for brand:  ({brand_urls})")


        # Navigate to the brand page
        driver.get(brand_link)

        # Find and extract the list of product links for the current brand
        product_links = driver.find_elements(By.CSS_SELECTOR, 'div.makers ul li a')
        #return product_links
        product_urls=[]
        for product_link in product_links[:3]:
            # Get the URL for the product page
            product_url = product_link.get_attribute('href')
            product_urls.append(product_url)
        page_numbers = driver.find_elements(By.CSS_SELECTOR, 'div.nav-pages a')
        pages=[]
        for page_number in page_numbers:
            page_url = page_number.get_attribute('href')
            print(page_url)
            pages.append(page_url)
        for page in pages:
            driver.get(page)
            sleep(2)  # Adjust sleep time if needed
            product_links = driver.find_elements(By.CSS_SELECTOR, 'div.makers ul li a')
            phone_urls = [product_link.get_attribute('href') for product_link in product_links]
            product_urls.extend(phone_urls)
        #return product_urls
        for product_link in product_urls[:3]:
            print(f"Extracting data for product:({product_urls})")
            # sleep(1)

            # Extract product specifications and add them to the results DataFrame
            product_data = return_specs_df(product_link,driver)
            #all_results = all_results.append(product_data, ignore_index=True)
            all_results=pd.concat([all_results,product_data],axis=0,ignore_index=True)
    driver.quit()

    return all_results



def return_stg_specs_df(driver):
    # Initialize the Chrome web driver
    # driver = webdriver.Chrome()
    # options = Options()
    # options.add_argument('--headless')
    # driver = webdriver.Chrome(options=options)

    # Navigate to the provided URL
    # driver.get(url)

    # Initialize an empty DataFrame to store the results
    all_results = pd.DataFrame()
    all_reviews=pd.DataFrame()

    for brand_link in brands_url:
        print(f"Extracting data for brand:  ({brand_link.name})")


        # Navigate to the brand page
        driver.get(brand_link.value)

        # Find and extract the list of product links for the current brand
        product_links = driver.find_elements(By.CSS_SELECTOR, 'div.makers ul li a')
        #return product_links
        product_urls=[]
        try:
            for product_link in product_links[:1]:
                # Get the URL for the product page
                product_url = product_link.get_attribute('href')
                product_urls.append(product_url)
            # page_numbers = driver.find_elements(By.CSS_SELECTOR, 'div.nav-pages a')
            # pages=[]
            # for page_number in page_numbers[:page_limit[brand_link.name].value]:
            #     page_url = page_number.get_attribute('href')
            #     print(page_url)
            #     pages.append(page_url)
            # for page in pages:
            #     driver.get(page)
            #     sleep(2)  # Adjust sleep time if needed
            #     product_links = driver.find_elements(By.CSS_SELECTOR, 'div.makers ul li a')
            #     phone_urls = [product_link.get_attribute('href') for product_link in product_links]
            #     product_urls.extend(phone_urls)
            for product_link in product_urls:
                print(f"Extracting data for product:({product_urls})")
                # sleep(1)

                # Extract product specifications and add them to the results DataFrame
                product_data = return_specs_df(product_link,driver)
                product_reviews=extract_all_reviews(product_link,driver)
                #all_results = all_results.append(product_data, ignore_index=True)
                all_results=pd.concat([all_results,product_data],axis=0,ignore_index=True)
                all_reviews=pd.concat([all_reviews,product_reviews],axis=0,ignore_index=True)
        except:
            continue
    driver.quit()

    return all_results,all_reviews


def return_stg_specs_exception_df(driver):
    all_results = pd.DataFrame()
    all_reviews = pd.DataFrame()

    for brand_link in brands_url:
        # driver = webdriver.Chrome()
        # options=Options()
        # options.add_argument('--headless')
        # driver = webdriver.Chrome(options=options)
        print(f"Extracting data for brand: ({brand_link.name})")

        # Initialize a variable for the number of retries
        max_retries = 3
        for retry in range(max_retries):
            try:
                driver.get(brand_link.value)
                break  # Exit the loop if the operation succeeds
            except WebDriverException:
                if retry == max_retries - 1:
                    # Handle the WebDriverException after maximum retries
                    print("WebDriverException: Maximum retries reached.")
                    return all_results, all_reviews
                else:
                    print(f"WebDriverException: Retrying... (Retry {retry + 1})")
                    sleep(2)  # Add a delay before retry

        product_urls = []  # Create an empty list to store product URLs
        try:
            # Extract product URLs for the current brand
            product_links = driver.find_elements(By.CSS_SELECTOR, 'div.makers ul li a')
            for product_link in product_links:
                product_url = product_link.get_attribute('href')
                product_urls.append(product_url)

            # Iterate through product URLs
            for product_url in product_urls[5:16]:
                print(f"Extracting data for product: ({product_url})")
                product_data = return_specs_df(product_url, driver)
                product_reviews = extract_all_reviews(product_url, driver)
                all_results = pd.concat([all_results, product_data], axis=0, ignore_index=True)
                all_reviews = pd.concat([all_reviews, product_reviews], axis=0, ignore_index=True)
        
        except:
            # driver.quit()
            continue  # Handle any exceptions and continue to the next brand
            
        # driver.quit()
    driver.quit()

    return all_results, all_reviews


def sentiment_analysis(review_text):
    text_blob = TextBlob(review_text)

# Perform sentiment analysis
    polarity = text_blob.sentiment.polarity  # Polarity ranges from -1 (negative) to 1 (positive)
    subjectivity = text_blob.sentiment.subjectivity  # Subjectivity ranges from 0 (objective) to 1 (subjective)

# Determine sentiment based on polarity
    if polarity > 0:
        sentiment = "Positive"
    elif polarity < 0:
        sentiment = "Negative"
    else:
        sentiment = "Neutral"   
    return sentiment



def ntlk_sentiment_analysis(review,analyzer):
# Initialize the VADER sentiment intensity analyzer
    # nltk.download("vader_lexicon")
    #analyzer = SentimentIntensityAnalyzer()

    # Example sentences for sentiment analysis
  

    # Perform sentiment analysis on each sentence

    sentiment_scores = analyzer.polarity_scores(review)
    compound_score = sentiment_scores["compound"]

    if compound_score >= 0.05:
        sentiment = "Positive"
    elif compound_score <= -0.05:
        sentiment = "Negative"
    else:
        sentiment = "Neutral"
    return sentiment

def sentiment_analysis_df(df):
    nltk.download("vader_lexicon")
    analyzer = SentimentIntensityAnalyzer()
    return_df=df.copy()
    return_df['Sentiment_textblob']=df['Review_Text'].apply(lambda x:sentiment_analysis(x))
    return_df['Sentiment_nltk']=df['Review_Text'].apply(lambda x:ntlk_sentiment_analysis(x,analyzer))
    return return_df


def retreive_sql_files(sql_command_directory_path):
    sql_files = [sqlfile for sqlfile in os.listdir(sql_command_directory_path) if sqlfile.endswith('.sql')]
    sorted_sql_files =  sorted(sql_files)
    return sorted_sql_files


def return_prices_df(url,driver):
    return_df=pd.DataFrame()
    driver.get(url)
    product_id=url.split('-')[1].split('.')[0]
    return_df.insert(0,'product_id',product_id)   
    try:
        list_df=pd.read_html(driver.page_source)
        result_df = pd.concat(list_df, ignore_index=True)
        result=result_df.iloc[-3:].T
        result.columns=result.iloc[0]
        data_row=result.iloc[1]
        return_df=pd.DataFrame([data_row])
        return_df.insert(0,'product_id',product_id)   
    except:
        pass
    finally:
        return return_df


def return_all_prices_df(driver,product_urls):
      all_results = pd.DataFrame()
      for product_url in product_urls:
            try:
                driver.get(product_url)
                print(f"Extracting data for product: ({product_url})")
                product_data = return_prices_df(product_url,driver)
                all_results = pd.concat([all_results, product_data], axis=0, ignore_index=True)
            except:
                continue
      driver.quit()
      return all_results

def return_tables_from_url(driver):
    # all_specs = pd.DataFrame()
    #all_reviews = pd.DataFrame()
    all_prices = pd.DataFrame()
    for product in product_url:
            try:
                driver.get(product.value)
                # print(f"Extracting data for product: ({product.value})")
                # product_specs = return_specs_df(product.value, driver)
                #product_reviews = extract_all_reviews(product.value,driver)
                product_prices = return_prices_df(product.value,driver)
                # all_specs = pd.concat([all_specs, product_specs], axis=0, ignore_index=True)
                #all_reviews = pd.concat([all_reviews, product_reviews], axis=0, ignore_index=True)
                all_prices = pd.concat([all_prices, product_prices], axis=0, ignore_index=True)
            except:
                continue
    driver.quit()
    return all_prices


def return_tables_reviews_from_url(driver):
    all_reviews = pd.DataFrame()
    for product in product_url:
            try:
                driver.get(product.value)
                print(f"Extracting data for product: ({product.value})")
                product_reviews = extract_all_reviews(product.value,driver)
                all_reviews = pd.concat([all_reviews, product_reviews], axis=0, ignore_index=True)
                
            except:
                continue
    driver.quit()
    return all_reviews

def convert_currency(currency_value):
    exchange_rates = {
        '€': 1.06,  
        '₹': 0.012,  
        '$':1,
        '£':1.22 
    }

    if not pd.notna(currency_value):
        return None

    # Extract the currency symbol
    currency_symbol = currency_value[0]

    # Remove the currency symbol, commas, and spaces, then convert to float
    amount = float(currency_value[1:].replace(',', '').strip())

    # Convert to USD using the exchange rate
    usd_amount = amount * exchange_rates.get(currency_symbol, 1.0)
    return usd_amount

def convert_currency_df(df):
    return_df=df.copy()
    for column in return_df.columns:
        if column != 'product_id':
            return_df[column] = return_df[column].apply(convert_currency)
    return return_df


def openai_sentiment_analysis(review,openai):
    response = openai.Completion.create(
    engine="text-davinci-003",
    prompt=(f"Sentiment analysis of the following text which is a smartphone review based on either positive,negative or neutral: '''''{review}'''''\n\nSentiment score: "),
    temperature=0,
    max_tokens=10
    )
    sentiment = response.choices[0].text.strip()
    return sentiment

def sentiment_analysis_df_openai(df,openai):
    return_df=df.copy()
    return_df['Sentiment']=return_df['Review_Text'].apply(lambda x:openai_sentiment_analysis(x,openai))
    return return_df

    
     



        

        





