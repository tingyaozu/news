import pandas as pd
from datetime import datetime, timedelta, date
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from webdriver_manager.firefox import GeckoDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import scrapy
from scrapy.crawler import CrawlerProcess
from database import insert_news
from langdetect import detect
import langid

def is_english(text):
    """Detect language using both langdetect and langid for better accuracy."""
    try:
        lang_detect = detect(text)  # langdetect method
        lang_langid, confidence = langid.classify(text)  # langid method
        final_decision = lang_detect == "en" or lang_langid == "en"
        # print(f"'{text}' → LangDetect: {lang_detect}, LangID: {lang_langid} (Confidence: {confidence:.2f}) → Final Decision: {final_decision}")
        return final_decision
    except Exception as e:
        print(f"Error detecting language for: {text}. Error: {e}")
        return False
    
def unstructured_news(target_date):
    """Scrapes hot news & market news from the website."""
    hot_news_list = []
    market_news_list = []

    options = Options()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    
    driver = webdriver.Firefox(executable_path=GeckoDriverManager().install(), options=options)
    driver.get("https://www.klsescreener.com/v2/news")

    def extract_hot_news(driver):
        """Extracts hot news headlines."""
        body = driver.page_source
        soup = BeautifulSoup(body, 'html.parser')
        hot_news_section = soup.find('div', class_='channel')
        lists = hot_news_section.find_all('li')

        for hot in lists:
            title = hot.find('a').get_text(strip=True)
            hyperlink = 'https://www.klsescreener.com' + hot.find('a')['href']
            date = hot.find_all('span')[1].get_text(strip=True)

            hot_news_list.append({
                'Title': title,
                'News Hyperlinks': hyperlink,
                'Published Date': date,
                'Related Stocks': None
            })

    extract_hot_news(driver)

    def extract_market_news():
        """Extracts market news within timeframe."""
        try:
            body = driver.page_source
            soup = BeautifulSoup(body, "html.parser")
            articles = soup.find("div", id="section")

            if not articles:
                return False

            last_date_element = articles.find_all("span", class_="moment-date")[-1]
            if not last_date_element or not last_date_element.get("data-date"):
                return False

            last_date_str = last_date_element["data-date"]
            last_date = datetime.fromisoformat(last_date_str).date()

            if target_date - last_date > timedelta(hours=3):  #要改
                articles_list = articles.find_all('div', class_='item figure flex-block')
                for article in articles_list:
                    news_hyperlink = 'https://www.klsescreener.com' + article.find('a')['href']
                    title = article.find('h2').get_text(strip=True)
                    date = article.find('span', attrs={"data-date": True})['data-date']

                    market_news_list.append({
                        'Title': title,
                        'News Hyperlinks': news_hyperlink,
                        'Published Date': date,
                        'Related Stocks': None
                    })
                return False

        except Exception as e:
            print(f"Error extracting market news: {e}")
            return False

        return True

    while extract_market_news():
        try:
            load_more_button = WebDriverWait(driver, 0.01).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, ".figure_loading"))
            )
            load_more_button.click()
        except TimeoutException:
            break
        except Exception as e:
            print(f"An error occurred: {e}")
            break

    driver.quit()
    return hot_news_list, market_news_list


class NewsScraper(scrapy.Spider):
    name = 'news_spider'

    def __init__(self, hot_news, market_news):
        self.hot_news = hot_news
        self.market_news = market_news
        self.start_urls = [item['News Hyperlinks'] for item in hot_news + market_news]

    def parse(self, response):
        """Extracts related stocks from each news article."""
        soup = BeautifulSoup(response.text, 'html.parser')
        related_stocks_section = soup.find('div', class_='stock-list table-responsive')

        related_stocks = []
        if related_stocks_section:
            related_stocks = [
                stock.find('span').get_text(strip=True)
                for stock in related_stocks_section.find_all('tr')
            ]

        for item in self.hot_news + self.market_news:
            if item['News Hyperlinks'] == response.url:
                item['Related Stocks'] = ', '.join(related_stocks)


if __name__ == "__main__":
    hot_news, market_news = unstructured_news(date.today())

    process = CrawlerProcess()
    process.crawl(NewsScraper, hot_news=hot_news, market_news=market_news)
    process.start()

    hot_news_df = pd.DataFrame(hot_news)
    market_news_df = pd.DataFrame(market_news)
    
    #drop non english 
    market_news_df['Language'] = market_news_df['Title'].apply(is_english)
    market_news_df = market_news_df[market_news_df['Language'] == True].drop(columns=['Language'])
    
    insert_news(market_news_df,'Market_News')

    print("✅ News scraping complete, data saved into sql.")
