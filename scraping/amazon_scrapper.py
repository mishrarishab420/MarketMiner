import scrapy
import pandas as pd
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from urllib.parse import urljoin, urlparse, parse_qs, unquote
import csv
import re
import random
import time
from collections import defaultdict
import os

class AmazonSpider(scrapy.Spider):
    name = 'amazon_spider'
    
    def __init__(self, query='laptop', pages=5, progress_callback=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.query = query
        self.progress_callback = progress_callback  # Store the progress callback
        
        if pages is None or pages < 1:
            self.start_urls = []
            return
            
        self.pages = pages
        formatted_query = query.replace(" ", "+")
        base_url = f"https://www.amazon.in/s?k={formatted_query}"
        # Append realistic params (crid, sprefix, ref can be random/static)
        params = "&crid=2K91KPZM8IIUC&sprefix=" + formatted_query + "%2Caps%2C236&ref=nb_sb_noss"
        
        if pages >= 1:
            self.start_urls = [f"{base_url}&page=1{params}"]
        else:
            self.start_urls = []
            
        self.product_urls = set()
        self.all_spec_keys = set()
        self.items = []
        self.scraped_count = 0
        
        # Estimate the number of expected items (approx 20 per page)
        self.expected_items = self.pages * 20
        
        # User agents for rotation
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:89.0) Gecko/20100101 Firefox/89.0'
        ]
        self.accept_languages = [
            "en-US,en;q=0.9",
            "en-GB,en;q=0.9",
            "en-CA,en;q=0.9",
            "en-AU,en;q=0.9",
            "en-NZ,en;q=0.9",
            "en-IE,en;q=0.9",
            "en-IN,en;q=0.9",
            "en-ZA,en;q=0.9",
            "en-SG,en;q=0.9",
            "en-PH,en;q=0.9",
            "en-HK,en;q=0.9",
            "en"
        ]
        self.referers = [
            "https://www.google.com/",
            "https://www.bing.com/",
            "https://duckduckgo.com/",
            "https://search.yahoo.com/"
        ]
    
        # Send initial progress update if callback is provided
        if self.progress_callback:
            self.progress_callback(10)  # 10% - Spider initialized
    
    def get_headers(self):
        return {
            "User-Agent": random.choice(self.user_agents),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": random.choice(self.accept_languages),
            "Referer": random.choice(self.referers),
            "DNT": "1",
            "Upgrade-Insecure-Requests": "1",
            "Cache-Control": "no-cache"
        }
    
    def start_requests(self):
        for i, url in enumerate(self.start_urls, 1):
            yield scrapy.Request(
                url,
                callback=self.parse,
                headers=self.get_headers(),
                meta={'page': i}
            )
    
    def parse(self, response):
        page = response.meta.get('page', 1)

        product_links = response.css(
            'div[data-component-type="s-search-result"] h2.a-size-base-plus a.a-link-normal::attr(href)'
        ).getall()
        
        for href in product_links:
            if href and ('/dp/' in href or '/gp/product/' in href):
                product_url = self.get_product_url(href)
                if product_url and product_url not in self.product_urls:
                    self.product_urls.add(product_url)
                    yield scrapy.Request(
                        product_url,
                        callback=self.parse_product,
                        headers=self.get_headers(),
                        priority=1,
                        meta={'page': page, 'brand_from_card': ''}
                    )
        
        # Show progress
        print(f"URLs found: {len(self.product_urls)} | Products scraped: {self.scraped_count}", end="\r")
    
        # Update progress based on URLs found (not scraped count)
        if self.progress_callback:
            # Calculate progress based on URLs found vs expected
            progress = 10 + min(30, (len(self.product_urls) / self.expected_items) * 30)
            self.progress_callback(int(progress))


    def get_product_url(self, href):
        """Extract actual product URL from Amazon's redirect URL"""
        # Handle Amazon's redirect URLs
        if href.startswith('/sspa/click?'):
            parsed = urlparse(href)
            query_params = parse_qs(parsed.query)
            if 'url' in query_params:
                actual_url = unquote(query_params['url'][0])
                # Extract the product ID part
                if '/dp/' in actual_url or '/gp/product/' in actual_url:
                    return urljoin('https://www.amazon.in', actual_url.split('?')[0])
        # Handle direct product URLs
        elif '/dp/' in href or '/gp/product/' in href:
            # Clean the URL by removing unnecessary parameters
            clean_url = href.split('?')[0] if '?' in href else href
            return urljoin('https://www.amazon.in', clean_url)
        return None
    
    def clean_text(self, text, default=''):
        """Clean and preprocess text data"""
        if not text:
            return default
        
        text = text.strip()
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text)
        return text
    
    def extract_price(self, price_text):
        """Extract numerical value from price text"""
        if not price_text:
            return ''
        
        # Remove currency symbols and commas
        price_text = re.sub(r'[^\d.]', '', price_text)
        try:
            return str(int(float(price_text)))
        except ValueError:
            return ''
    
    def extract_discount(self, discount_text):
        """Extract and clean discount percentage"""
        if not discount_text:
            return ''
        
        # Remove any non-digit characters except minus and percentage
        discount_text = re.sub(r'[^\d%-]', '', discount_text)
        
        # Remove minus sign if present
        discount_text = discount_text.replace('-', '')
        
        # Ensure it ends with percentage sign
        if discount_text and not discount_text.endswith('%'):
            discount_text += '%'
            
        return discount_text
    
    def extract_rating(self, rating_text):
        """Extract numerical rating value"""
        if not rating_text:
            return ''
        
        # Extract just the numeric part
        match = re.search(r'(\d+\.\d+)', rating_text)
        if match:
            return match.group(1)
        return ''
    
    def extract_reviews_count(self, reviews_text):
        """Extract numerical reviews count"""
        if not reviews_text:
            return ''
        
        # Extract numbers from text
        numbers = re.findall(r'[\d,]+', reviews_text)
        if numbers:
            try:
                return numbers[0].replace(',', '')
            except ValueError:
                return ''
        return ''
    
    def preprocess_data(self, item):
        """Preprocess and clean the extracted data"""
        # Clean text fields
        for field in ['Title', 'Brand', 'MRP', 'Current Price', 'Discount', 'Rating', 'Reviews']:
            if field in item:
                item[field] = self.clean_text(item[field])

        # Process specific fields with consistent keys
        item['MRP'] = self.extract_price(item.get('MRP', ''))
        item['Current Price'] = self.extract_price(item.get('Current Price', ''))
        item['Discount'] = self.extract_discount(item.get('Discount', ''))
        item['Rating'] = self.extract_rating(item.get('Rating', ''))
        item['Reviews'] = self.extract_reviews_count(item.get('Reviews', ''))

        try:
            mrp_val = int(item['MRP']) if item['MRP'] else None
            current_val = int(item['Current Price']) if item['Current Price'] else None
            discount_val = int(item['Discount'].replace('%','')) if item['Discount'] else None

            if mrp_val and discount_val is not None:
                expected_price = int(round(mrp_val * (1 - discount_val/100)))
                if current_val:
                    lower_bound = expected_price * 0.95
                    upper_bound = expected_price * 1.05
                    if not (lower_bound <= current_val <= upper_bound):
                        item['Current Price'] = str(expected_price)
                else:
                    item['Current Price'] = str(expected_price)
        except Exception:
            pass

        return item
    
    def parse_product(self, response):
        try:
            # Extract basic product information
            title = response.css('span#productTitle::text').get()
            title = self.clean_text(title) if title else ''
            
            brand = response.meta.get('brand_from_card', '')
            if not brand:
                brand_from_specs = response.css('tr.po-brand td.a-span9 span::text').get()
                if brand_from_specs:
                    brand = self.clean_text(brand_from_specs)
            
            # Extract MRP
            mrp_element = response.css('span.a-price.a-text-price[data-a-strike="true"] span.a-offscreen::text').get()
            mrp = self.clean_text(mrp_element) if mrp_element else ''
            
            # Extract current price
            current_price_element = response.css('span.a-price-whole::text').get()
            current_price = self.clean_text(current_price_element) if current_price_element else ''
            
            # Extract discount percentage
            discount_element = response.css('span.savingsPercentage::text').get()
            discount = self.clean_text(discount_element) if discount_element else ''
            
            # Extract rating
            rating_element = response.css('span.a-icon-alt::text').get()
            rating = self.clean_text(rating_element) if rating_element else ''
            
            # Extract number of reviews
            reviews_element = response.css('span#acrCustomerReviewText::text').get()
            reviews = self.clean_text(reviews_element) if reviews_element else ''
            
            # Extract specifications
            specs = {}
            spec_rows = response.css('table.a-normal.a-spacing-micro tr')
            for row in spec_rows:
                key_element = row.css('td.a-span3 span::text')
                value_element = row.css('td.a-span9 span::text')
                
                if key_element.get() and value_element.get():
                    key = self.clean_text(key_element.get())
                    value = self.clean_text(value_element.get())
                    specs[key] = value
                    self.all_spec_keys.add(key)
            
            if not specs:
                fact_rows = response.css('div.a-section[role="list"] div.product-facts-detail')
                for row in fact_rows:
                    key_element = row.css('div.a-col-left span.a-color-base::text').get()
                    value_element = row.css('div.a-col-right span.a-color-base::text').get()
                    
                    if key_element and value_element:
                        key = self.clean_text(key_element)
                        value = self.clean_text(value_element)
                        specs[key] = value
                        self.all_spec_keys.add(key)
            
            # Avoid duplication if brand is already extracted
            if brand and "Brand" in specs:
                del specs["Brand"]
            
            # Store the product data
            item = {
                'URL': response.url,
                'Title': title,
                'Brand': brand,
                'MRP': mrp,
                'Current Price': current_price,
                'Discount': discount,
                'Rating': rating,
                'Reviews': reviews,
                'specs': specs
            }
            
            # Preprocess the data
            item = self.preprocess_data(item)
            self.items.append(item)
            
            # Update scraped count and show progress
            self.scraped_count += 1
            print(f"URLs found: {len(self.product_urls)} | Products scraped: {self.scraped_count}", end="\r")
            
            yield item
            
        except Exception as e:
            print(f"Error: {type(e).__name__}")
    
    def closed(self, reason):
        # After spider closes, write data to CSV
        self.write_to_csv()
        print(f"\nScraping completed. URLs: {len(self.product_urls)}, Products: {self.scraped_count}")
    
    def write_to_csv(self):
        # Create CSV with dynamic columns based on collected specifications
        constant_fields = ['URL', 'Title', 'Brand', 'MRP', 'Current Price', 'Discount', 'Rating', 'Reviews']

        valid_spec_keys = []
        threshold = max(1, int(0.4 * len(self.items)))
        for spec_key in self.all_spec_keys:
            count = sum(1 for item in self.items if item['specs'].get(spec_key))
            if count >= threshold:
                valid_spec_keys.append(spec_key)

        fieldnames = constant_fields + sorted(valid_spec_keys)

        # Ensure data directory exists
        if not os.path.exists('data'):
            os.makedirs('data')

        filename = os.path.join('data', f'amazon_{self.query}_products.csv')
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for item in self.items:
                row = {field: item.get(field, '') for field in constant_fields}
                for spec_key in valid_spec_keys:
                    row[spec_key] = item['specs'].get(spec_key, '')
                writer.writerow(row)

# Add this function at the end of amazon_scrapper.py
def scrape_amazon(query, pages, progress_callback=None):
    """Function to scrape Amazon and return a DataFrame"""
    # Update progress at various stages
    if progress_callback:
        progress_callback(10)  # 10% - Spider started

    # Import here to avoid reactor issues
    import os
    import sys
    from scrapy.crawler import CrawlerProcess
    from scrapy.utils.project import get_project_settings
    
    # Set the SCRAPY_SETTINGS_MODULE environment variable
    os.environ.setdefault('SCRAPY_SETTINGS_MODULE', 'scraping.settings')
    
    # Add the current directory to Python path
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    
    settings = get_project_settings()
    # Configure settings
    settings.set('USER_AGENT', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
    settings.set('CONCURRENT_REQUESTS', 4)
    settings.set('CONCURRENT_REQUESTS_PER_DOMAIN', 4)
    settings.set('DOWNLOAD_DELAY', 1)
    settings.set('AUTOTHROTTLE_ENABLED', True)
    settings.set('AUTOTHROTTLE_START_DELAY', 1)
    settings.set('AUTOTHROTTLE_MAX_DELAY', 3)
    settings.set('AUTOTHROTTLE_TARGET_CONCURRENCY', 2.0)
    settings.set('RETRY_ENABLED', True)
    settings.set('RETRY_TIMES', 3)
    settings.set('COOKIES_ENABLED', False)
    settings.set('DOWNLOAD_TIMEOUT', 30)
    settings.set("LOG_LEVEL", "ERROR")
    
    # Use CrawlerProcess instead of CrawlerRunner for better thread compatibility
    process = CrawlerProcess(settings)
    
    # Create a custom pipeline to collect items
    items = []
    
    class ItemCollectorPipeline:
        def process_item(self, item, spider):
            items.append(item)
            # Update progress based on number of items collected
            if progress_callback and hasattr(spider, 'expected_items'):
                progress = 10 + min(80, (len(items) / spider.expected_items) * 80)
                progress_callback(int(progress))
            return item
    
    # Add the custom pipeline to settings
    settings.set('ITEM_PIPELINES', {
        'scraping.amazon_scrapper.ItemCollectorPipeline': 100,
    })
    
    # Run the spider
    process.crawl(AmazonSpider, query=query, pages=pages, progress_callback=progress_callback)
    process.start()
    
    if progress_callback:
        progress_callback(90)  # 90% - Scraping complete, converting to DataFrame
    
    # Convert items to DataFrame
    if not items:
        return pd.DataFrame()
    
    # Convert the scraped items to a DataFrame
    df_data = []
    for item in items:
        row = {
            'Product': item.get('Title', ''),
            'Price (₹)': item.get('Current Price', ''),
            'Rating': item.get('Rating', ''),
            'Reviews': item.get('Reviews', ''),
            'Brand': item.get('Brand', ''),
            'Discount %': item.get('Discount', '').replace('%', '') if item.get('Discount') else '',
            'MRP': item.get('MRP', ''),
            'URL': item.get('URL', '')
        }
        # Add specifications as columns
        for key, value in item.get('specs', {}).items():
            row[key] = value
        df_data.append(row)
    
    if progress_callback:
        progress_callback(100)  # 100% - Complete
    
    return pd.DataFrame(df_data)