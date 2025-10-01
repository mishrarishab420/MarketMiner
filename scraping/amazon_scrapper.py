import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from urllib.parse import urljoin, urlparse, parse_qs, unquote
import csv
import re
import random
import time
from collections import defaultdict

class AmazonSpider(scrapy.Spider):
    name = 'amazon_spider'
    
    def __init__(self, query='laptop', pages=5, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.query = query
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
        for url in self.start_urls:
            yield scrapy.Request(
                url,
                callback=self.parse,
                headers=self.get_headers(),
                meta={'page': 1}
            )
    
    def parse(self, response):
        page = response.meta.get('page', 1)
        
        product_cards = response.css('div[data-component-type="s-search-result"]')
        for card in product_cards:
            href = card.css('a.a-link-normal.s-link-style.a-text-normal::attr(href)').get()
            if href and ('/dp/' in href or '/gp/product/' in href):
                product_url = self.get_product_url(href)
                if product_url and product_url not in self.product_urls:
                    brand_snippet = card.css('div.a-row.a-size-base.a-color-secondary h2 span::text').get()
                    brand_from_card = self.clean_text(brand_snippet) if brand_snippet else ''
                    self.product_urls.add(product_url)
                    yield scrapy.Request(
                        product_url,
                        callback=self.parse_product,
                        headers=self.get_headers(),
                        priority=1,
                        meta={'page': page, 'brand_from_card': brand_from_card}
                    )
        
        # Handle pagination
        if page < self.pages:
            next_page = response.css('a.s-pagination-next::attr(href)').get()
            if next_page:
                next_page_url = urljoin('https://www.amazon.in', next_page)
                yield scrapy.Request(
                    next_page_url, 
                    callback=self.parse,
                    headers=self.get_headers(),
                    meta={'page': page + 1}
                )
    
    def get_product_url(self, href):
        """Extract actual product URL from Amazon's redirect URL"""
        if href.startswith('/sspa/click?'):
            parsed = urlparse(href)
            query_params = parse_qs(parsed.query)
            if 'url' in query_params:
                actual_url = unquote(query_params['url'][0])
                return urljoin('https://www.amazon.in', actual_url)
        elif href.startswith('/'):
            return urljoin('https://www.amazon.in', href)
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
        """Extract and clean discount percentage as numeric string (no % sign)"""
        if not discount_text:
            return ''
        # Remove any non-digit characters (keep only digits)
        discount_text = re.sub(r'[^\d]', '', discount_text)
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
        """Preprocess and clean the extracted data with robust price/discount checks"""
        # Clean text fields
        for field in ['Title', 'Brand', 'MRP', 'Current Price', 'Discount', 'Rating', 'Reviews']:
            if field in item:
                item[field] = self.clean_text(item[field])

        # Extract numeric values
        mrp_val = self.extract_price(item.get('MRP', ''))
        current_val = self.extract_price(item.get('Current Price', ''))
        discount_val = self.extract_discount(item.get('Discount', ''))

        mrp_val = int(mrp_val) if mrp_val else None
        current_val = int(current_val) if current_val else None
        discount_val_num = int(discount_val) if discount_val else None

        # --- Custom rules ---
        # 1. If MRP is missing but Current Price is available, set MRP = Current Price, Discount = 0
        if (mrp_val is None or mrp_val == 0) and (current_val is not None and current_val != 0):
            mrp_val = current_val
            discount_val_num = 0
        # 2. If discount is negative, set Current Price = MRP and Discount = 0
        if (
            discount_val_num is not None
            and discount_val_num < 0
            and mrp_val is not None
        ):
            current_val = mrp_val
            discount_val_num = 0

        # Fill missing values based on available columns
        try:
            if current_val is None and mrp_val is not None and discount_val_num is not None:
                current_val = round(mrp_val * (1 - discount_val_num / 100))
            if discount_val_num is None and mrp_val is not None and current_val is not None and mrp_val != 0:
                discount_val_num = round((mrp_val - current_val) / mrp_val * 100)
            if mrp_val is None and current_val is not None and discount_val_num is not None and discount_val_num != 100:
                mrp_val = round(current_val / (1 - discount_val_num / 100))
        except Exception:
            pass

        # Correct Current Price if it deviates >5% from expected
        try:
            if mrp_val is not None and discount_val_num is not None:
                expected_price = round(mrp_val * (1 - discount_val_num / 100))
                if current_val is not None:
                    lower_bound = expected_price * 0.95
                    upper_bound = expected_price * 1.05
                    if not (lower_bound <= current_val <= upper_bound):
                        current_val = expected_price
                else:
                    current_val = expected_price
        except Exception:
            pass

        # Update item with final values
        item['MRP'] = str(mrp_val) if mrp_val is not None else ''
        item['Current Price'] = str(current_val) if current_val is not None else ''
        # Discount numeric value only (no % sign)
        item['Discount'] = str(discount_val_num) if discount_val_num is not None else ''

        # Process rating and reviews as before
        item['Rating'] = self.extract_rating(item.get('Rating', ''))
        item['Reviews'] = self.extract_reviews_count(item.get('Reviews', ''))

        return item
    
    def parse_product(self, response):
        try:
            # Extract basic product information
            title = response.css('span#productTitle::text').get()
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
        # Constant fields, with 'Discount %' as the column header
        constant_fields = ['URL', 'Title', 'Brand', 'MRP', 'Current Price', 'Discount %', 'Rating', 'Reviews']
        placeholder = 'N/A'

        # Step 1: Count non-empty entries for each spec key across all items
        spec_counts = defaultdict(int)
        total_items = len(self.items)
        for item in self.items:
            specs = item.get('specs', {})
            for key in self.all_spec_keys:
                value = specs.get(key, '')
                if value and str(value).strip():
                    spec_counts[key] += 1

        # Step 2: Keep only spec keys with at least 40% non-empty data
        threshold = int(0.4 * total_items)
        filtered_spec_keys = sorted([k for k in self.all_spec_keys if spec_counts[k] >= threshold])

        # Remove duplicate spec keys while preserving order
        seen = set()
        unique_filtered_spec_keys = []
        for key in filtered_spec_keys:
            if key not in seen:
                unique_filtered_spec_keys.append(key)
                seen.add(key)

        # Step 3: Write the CSV using these filtered keys along with constant fields (no duplicate columns)
        fieldnames = constant_fields + unique_filtered_spec_keys
        import os
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        data_dir = os.path.join(base_dir, "data")
        os.makedirs(data_dir, exist_ok=True)
        filename = os.path.join(data_dir, f'amazon_{self.query}_products.csv')
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for item in self.items:
                # Skip rows where 'Brand' is missing or empty
                brand_val = item.get('Brand', '')
                if brand_val is None or str(brand_val).strip() == '':
                    continue
                # Fill constant fields with placeholder if missing or empty
                row = {}
                for field in constant_fields:
                    # For 'Discount %', use the numeric value from 'Discount'
                    if field == 'Discount %':
                        val = item.get('Discount', '')
                    else:
                        val = item.get(field, '')
                    if val is None or str(val).strip() == '':
                        row[field] = placeholder
                    else:
                        row[field] = val
                # Fill spec fields with placeholder if missing or empty
                specs = item.get('specs', {}) if isinstance(item.get('specs', {}), dict) else {}
                for key in unique_filtered_spec_keys:
                    sval = specs.get(key, '')
                    if sval is None or str(sval).strip() == '':
                        row[key] = placeholder
                    else:
                        row[key] = sval
                writer.writerow(row)

if __name__ == "__main__":
    # Configure Scrapy settings for maximum speed
    settings = get_project_settings()
    settings.set('USER_AGENT', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
    settings.set('CONCURRENT_REQUESTS', 100)  # Increased significantly
    settings.set('CONCURRENT_REQUESTS_PER_DOMAIN', 16)  # Increased
    settings.set('DOWNLOAD_DELAY', 0)  # Remove delay
    settings.set('AUTOTHROTTLE_ENABLED', False)  # Disable auto-throttling
    settings.set('RANDOMIZE_DOWNLOAD_DELAY', False)  # Disable random delays
    settings.set('RETRY_ENABLED', True)
    settings.set('RETRY_TIMES', 2)  # Reduce retries
    settings.set('COOKIES_ENABLED', False)
    settings.set('DOWNLOAD_TIMEOUT', 15)  # Reduce timeout
    settings.set('LOG_LEVEL', 'ERROR')
    
    # Additional performance optimizations
    settings.set('REACTOR_THREADPOOL_MAXSIZE', 20)  # Increase thread pool
    settings.set('DNS_TIMEOUT', 10)  # Reduce DNS timeout
    settings.set('DOWNLOAD_MAXSIZE', 0)  # No limit on response size
    settings.set('REDIRECT_ENABLED', True)
    settings.set('REDIRECT_MAX_TIMES', 2)  # Reduce redirects
    settings.set('AJAXCRAWL_ENABLED', False)  # Disable AJAX crawling
    settings.set('TELNETCONSOLE_ENABLED', False)  # Disable telnet
    
    # Get user input
    search_query = input("Enter search query (e.g., 'laptop'): ").strip()
    num_pages = input("Enter number of pages to scrape (e.g., 5): ").strip()
    if not num_pages.isdigit() or int(num_pages) < 1:
        print("No valid page number entered. Exiting without scraping.")
    else:
        num_pages = int(num_pages)
        process = CrawlerProcess(settings)
        process.crawl(AmazonSpider, query=search_query, pages=num_pages)
        process.start()