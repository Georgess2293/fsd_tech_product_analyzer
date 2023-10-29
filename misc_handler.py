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
from lookups import brands_url,page_limit,product_url,sales_url,staging_tables
from time import sleep
from datetime import datetime
import pandas as pd
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from textblob import TextBlob

def return_url_gsm_search(input_text,driver):
    try:
        url=f'https://www.gsmarena.com/res.php3?sSearch={input_text}'
        driver.get(url)
        product_links = driver.find_elements(By.CSS_SELECTOR, 'div.makers a')
        return_url= product_links[0].get_attribute('href')
        return return_url
    except:
        print("No product found")


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
    sleep(2)
    driver.get(url)
    #review_elements = driver.find_elements(By.CLASS_NAME, 'user-thread')
    product_id=url.split('-reviews-')[1].split('p')[0]
    try:
        name = WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="body"]/div/div[1]/div/div[1]/h1'))).text
    except:
        name=None
    review_elements = driver.find_elements(By.CLASS_NAME, 'user-thread')
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
    # driver = webdriver.Chrome()
    # options=Options()
    # options.add_argument('--headless')
    # driver = webdriver.Chrome(options=options)
    all_reviews_data = []
    page_number=1
    url_reviews=url.split('-')[0]+'-reviews-'+url.split('-')[1]
    while True:
        current=url_reviews.split('.php')[0]
        current_url=current+'p'+str(page_number)+'.php'

        reviews_on_page = extract_reviews_from_page(current_url,driver)
        if not reviews_on_page:
            break

        all_reviews_data.extend(reviews_on_page)
        page_number += 1
    all_reviews_data_df=pd.DataFrame(all_reviews_data)
    #driver.quit()
    return all_reviews_data_df
    

def extract_reddit_comments(search_query, reddit,num_results=3):
    post_titles=[]
    usernames = []
    dates = []
    comments = []

 
    search_results = reddit.subreddit("all").search(search_query, limit=num_results)

    for submission in search_results:
        if search_query.lower() in submission.title.lower():
            post_title = submission.title
            submission.comments.replace_more(limit=None)
            for comment in submission.comments.list():
                post_titles.append(post_title)
                usernames.append(comment.author)
                dates.append(comment.created_utc)
                comments.append(comment.body)

    data = {
        'Post Title': post_titles,
        'User Name': usernames,
        'Date': [pd.to_datetime(date, unit='s') for date in dates],
        'Review Text': comments,
    }
    df = pd.DataFrame(data)

    return df
  

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


def ntlk_sentiment_analysis(review,analyzer):
    # nltk.download("vader_lexicon")
    #analyzer = SentimentIntensityAnalyzer()

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
    #nltk.download("vader_lexicon")
    analyzer = SentimentIntensityAnalyzer()
    return_df=df.copy()
    #return_df['Sentiment_textblob']=df['Review_Text'].apply(lambda x:sentiment_analysis(x))
    return_df['Sentiment_nltk']=df['Review_Text'].apply(lambda x:ntlk_sentiment_analysis(x,analyzer))
    return return_df


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


def convert_currency(currency_value):
    exchange_rates = {
        '€': 1.06,  
        '₹': 0.012,  
        '$':1,
        '£':1.22 
    }

    if not pd.notna(currency_value):
        return None
    currency_symbol = currency_value[0]
    amount = float(currency_value[1:].replace(',', '').strip())
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

def return_sales_per_year(driver):
    driver.get(sales_url.url.value)
    nav_bar = driver.find_element(By.XPATH,'//*[@id="smartphones-sold-each-year-by-manufacturer"]/div[2]/div[2]/div/div/ul')
    nav_options = nav_bar.find_elements(By.TAG_NAME,'a')
    nav_links = [option.get_attribute('href') for option in nav_options]
    nav_texts = [option.text for option in nav_options]
    all_dataframes = []
    odd_numbers = list(range(1, 32, 2))
    table = driver.find_element(By.XPATH, '//*[@id="smartphones-sold-each-year-by-manufacturer"]/div[2]/div[2]/div/div/div')
    for i,j in zip(odd_numbers,nav_texts):
        df=pd.read_html(table.get_attribute('outerHTML'))[i]
        df['Year'] = j
        all_dataframes.append(df)
        #print(f"Year: {nav_texts[i]}, URL: {nav_link}")
    combined_df = pd.concat(all_dataframes, ignore_index=True)
    driver.quit()
    return combined_df

def return_stg_tables_as_list():
    tables=[]
    for table in staging_tables:
        tables.append(table.value)
    return tables



    
     



        

        





