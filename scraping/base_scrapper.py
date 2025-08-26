import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import logging

class BaseScraper:
    def __init__(self, base_url, headers=None, delay_range=(1, 3)):
        self.base_url = base_url
        self.user_agents = [
            # macOS Chrome
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36",

            # macOS Safari
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/605.1.15 (KHTML, like Gecko) "
            "Version/17.0 Safari/605.1.15",

            # Windows Chrome
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36",

            # Windows Edge
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36 Edg/124.0.2478.67",

            # Linux Chrome
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36",

            # Android Chrome
            "Mozilla/5.0 (Linux; Android 14; Pixel 7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Mobile Safari/537.36",

            # iPhone Safari
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
            "AppleWebKit/605.1.15 (KHTML, like Gecko) "
            "Version/17.0 Mobile/15E148 Safari/604.1",
        ]
        self.delay_range = delay_range
        logging.basicConfig(filename="logs/app.log", level=logging.INFO)

    def get_page(self, url):
        """Fetch a single page and return BeautifulSoup object."""
        try:
            headers = {"User-Agent": random.choice(self.user_agents)}
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            logging.info(f"Fetched URL: {url}")
            return BeautifulSoup(response.text, "html.parser")
        except Exception as e:
            logging.error(f"Error fetching {url}: {e}")
            return None

    def random_delay(self):
        """Delay between requests to avoid blocking."""
        delay = random.uniform(*self.delay_range)
        time.sleep(delay)

    def save_to_dataframe(self, data, columns=None):
        if not data:
            return pd.DataFrame(columns=columns or [])
        if columns is None:
            columns = list(data[0].keys())
        return pd.DataFrame(data, columns=columns)

    def run(self):
        """Main method to be overridden in child classes."""
        raise NotImplementedError("You must override the run() method in the scraper class.")
    
    def get_page_with_retry(self, url, retries=3, backoff=2):
        """Fetch page with retry logic and exponential backoff, using random User-Agent."""
        for attempt in range(retries):
            try:
                headers = {"User-Agent": random.choice(self.user_agents)}
                response = requests.get(url, headers=headers, timeout=10)
                response.raise_for_status()
                return response.text
            except Exception as e:
                time.sleep(backoff ** attempt)
        return None
    
    def clean_text(self, text):
        """Strip and normalize text."""
        return text.strip() if text else ""

    def extract_price(self, text):
        """Extract numeric price from string like '₹1,299'."""
        try:
            return float("".join(filter(str.isdigit, text)))
        except:
            return None
        
    def save_to_csv(self, dataframe, filepath):
        """Save DataFrame to CSV."""
        dataframe.to_csv(filepath, index=False)
        logging.info(f"Data saved to {filepath}")

    def safe_extract(self, element, attr=None):
        try:
            return element[attr].strip() if attr else element.get_text(strip=True)
        except:
            return None