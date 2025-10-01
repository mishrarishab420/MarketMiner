import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from urllib.parse import urljoin, urlparse, parse_qs, unquote
import csv
import re
import random
import time
from collections import defaultdict

class FlipkartSpider(scrapy.Spider):
    name = 'flipkart_spider'
    
    def __init__(self, query='laptop', pages=5, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.query = query
        if pages is None or pages < 1:
            self.start_urls = []
            return
        self.pages = pages
        formatted_query = query.replace(" ", "+")
        base_url = f"https://www.flipkart.com/search?q={formatted_query}"
        
        if pages >= 1:
            self.start_urls = [base_url]
        else:
            self.start_urls = []
        self.product_urls = set()
        self.all_spec_keys = set()
        self.items = []
        self.scraped_count = 0
        self.current_page = 1
        
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
                callback=self.parse_search_page,
                headers=self.get_headers(),
                meta={'page': 1}
            )
    
    def parse_search_page(self, response):
        page = response.meta.get('page', 1)
        print(f"🔍 Scraping page {page}...")
        
        # Extract ALL product links using multiple strategies
        product_links = self.extract_all_product_links(response)
        print(f"📦 Found {len(product_links)} products on page {page}")
        
        # Process each product link
        for link in product_links:
            product_url = urljoin('https://www.flipkart.com', link)
            if product_url not in self.product_urls:
                self.product_urls.add(product_url)
                yield scrapy.Request(
                    product_url,
                    callback=self.parse_product,
                    headers=self.get_headers(),
                    priority=1,
                    meta={'page': page}
                )
        
        # Handle pagination - FIXED VERSION
        if page < self.pages:
            next_page = page + 1
            print(f"➡️ Moving to page {next_page}...")
            
            # Build next page URL properly
            next_page_url = f"https://www.flipkart.com/search?q={self.query.replace(' ', '+')}&page={next_page}"
            
            yield scrapy.Request(
                next_page_url,
                callback=self.parse_search_page,
                headers=self.get_headers(),
                meta={'page': next_page}
            )
        else:
            print(f"✅ All {self.pages} pages processed!")
    
    def extract_all_product_links(self, response):
        """Extract ALL product links using multiple strategies"""
        all_links = []
        
        # Strategy 1: All links containing /p/ pattern
        p_links = response.css('a[href*="/p/"]::attr(href)').getall()
        all_links.extend(p_links)
        
        # Strategy 2: Links from common product containers
        product_containers = [
            'div[data-id]',
            'div[class*="_1AtVbE"]',
            'div[class*="_2kHMtA"]', 
            'div[class*="_4ddWXP"]',
            'div[class*="_1xHGtK"]',
            'div[class*="_13oc-S"]',
            'div[class*="tUxRFH"]'
        ]
        
        for container in product_containers:
            links = response.css(f'{container} a::attr(href)').getall()
            all_links.extend(links)
        
        # Strategy 3: Direct product links with common classes
        direct_links = response.css('a[class*="_1fQZEK"], a[class*="wjcEIp"], a[class*="s1Q9rs"], a[class*="_2UzuFa"]::attr(href)').getall()
        all_links.extend(direct_links)
        
        # Strategy 4: Any link that looks like a product link
        all_a_tags = response.css('a::attr(href)').getall()
        for link in all_a_tags:
            if link and ('/p/' in link or '/product/' in link):
                all_links.append(link)
        
        # Clean and deduplicate links
        clean_links = []
        for link in all_links:
            if link and link.startswith('/'):
                # Remove query parameters to get clean product URL
                clean_link = link.split('?')[0] if '?' in link else link
                clean_links.append(clean_link)
        
        # Remove duplicates while preserving order
        seen = set()
        unique_links = []
        for link in clean_links:
            if link not in seen:
                seen.add(link)
                unique_links.append(link)
        
        return unique_links
    
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
        """Extract numerical rating value - handles both float and integer ratings"""
        if not rating_text:
            return ''
        
        # Extract just the numeric part (supports both integers and floats)
        match = re.search(r'(\d+(?:\.\d+)?)', rating_text)
        if match:
            rating_value = match.group(1)
            # Convert to float and back to string to ensure consistent format
            try:
                # If it's an integer, add .0 to make it consistent
                if '.' not in rating_value:
                    return f"{rating_value}.0"
                return rating_value
            except ValueError:
                return ''
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
    
    def parse_specifications_type1(self, response):
        """Parse specifications from first type (table structure)"""
        specs = {}
        
        try:
            # First type: table structure with specific classes
            spec_rows = response.css('table tr')
            for row in spec_rows:
                # Try multiple selector patterns for key
                key_selectors = [
                    'td:first-child span::text',
                    'td:first-child::text', 
                    'td[class*="col-3"] span::text',
                    'td[class*="col-3"]::text'
                ]
                
                value_selectors = [
                    'td:last-child li::text',
                    'td:last-child span::text',
                    'td:last-child::text',
                    'td[class*="col-9"] li::text',
                    'td[class*="col-9"] span::text'
                ]
                
                key = None
                value = None
                
                for key_selector in key_selectors:
                    key = row.css(key_selector).get()
                    if key and self.clean_text(key):
                        key = self.clean_text(key)
                        break
                
                for value_selector in value_selectors:
                    value = row.css(value_selector).get()
                    if value and self.clean_text(value):
                        value = self.clean_text(value)
                        break
                
                if key and value:
                    specs[key] = value
                    self.all_spec_keys.add(key)
        except Exception as e:
            print(f"Error in type1 specs: {e}")
        
        return specs
    
    def parse_specifications_type2(self, response):
        """Parse specifications from second type (row structure)"""
        specs = {}
        
        try:
            # Second type: div row structure
            spec_rows = response.css('div[class*="row"]')
            for row in spec_rows:
                # Try multiple selector patterns
                key_selectors = [
                    'div[class*="col-3"]::text',
                    'div:first-child::text'
                ]
                
                value_selectors = [
                    'div[class*="col-9"]::text', 
                    'div:last-child::text'
                ]
                
                key = None
                value = None
                
                for key_selector in key_selectors:
                    key = row.css(key_selector).get()
                    if key and self.clean_text(key):
                        key = self.clean_text(key)
                        break
                
                for value_selector in value_selectors:
                    value = row.css(value_selector).get()
                    if value and self.clean_text(value):
                        value = self.clean_text(value)
                        break
                
                if key and value and len(key) < 50:  # Avoid very long keys which are probably not spec keys
                    specs[key] = value
                    self.all_spec_keys.add(key)
        except Exception as e:
            print(f"Error in type2 specs: {e}")
        
        return specs
    
    def extract_mrp_properly(self, response):
        """Extract MRP properly when multiple elements exist"""
        try:
            # Try multiple MRP selectors
            mrp_selectors = [
                'div.yRaY8j.A6\\+E6v::text',  # Escaped + sign
                'div[class*="yRaY8j"]::text',
                'div._3I9_wc::text',
                'div._3I9_wc._2p6lqe::text'
            ]
            
            all_mrp_elements = []
            for selector in mrp_selectors:
                elements = response.css(selector).getall()
                all_mrp_elements.extend(elements)
            
            # Clean and get the highest price as MRP (usually MRP is higher)
            mrp_prices = []
            for mrp_text in all_mrp_elements:
                clean_mrp = self.clean_text(mrp_text)
                if clean_mrp:
                    price_num = self.extract_price(clean_mrp)
                    if price_num:
                        mrp_prices.append(int(price_num))
            
            if mrp_prices:
                # Return the highest price as MRP
                return str(max(mrp_prices))
            
            return ''
        except Exception as e:
            print(f"Error extracting MRP: {e}")
            return ''
    
    def extract_rating_properly(self, response):
        """Extract rating with better methods - handles both float and integer ratings"""
        try:
            # Multiple rating selectors
            rating_selectors = [
                'div.XQDdHH::text',
                'div._3LWZlK::text',
                'span._1lRcqv::text',
                'div[class*="rating"]::text'
            ]
            
            for selector in rating_selectors:
                rating_text = response.css(selector).get()
                if rating_text:
                    rating = self.extract_rating(rating_text)
                    if rating:
                        return rating
            
            # If no rating found, try to find in the rating span
            rating_span = response.css('span.Y1HWO0 div::text').get()
            if rating_span:
                return self.extract_rating(rating_span)
            
            # NEW: Try to extract from image-based ratings (like your example)
            # Look for div containing both number and image
            rating_divs = response.css('div.XQDdHH')
            for div in rating_divs:
                # Get all text nodes within this div
                all_text = ''.join(div.css('*::text').getall()).strip()
                if all_text:
                    rating = self.extract_rating(all_text)
                    if rating:
                        return rating
            
            return ''
        except Exception as e:
            print(f"Error extracting rating: {e}")
            return ''
    
    def extract_ratings_and_reviews_separately(self, response):
        """Extract ratings count and reviews count separately"""
        ratings_count = ''
        reviews_count = ''
        
        try:
            # Extract from the Wphh3N span structure
            ratings_reviews_text = response.css('span.Wphh3N span::text').getall()
            
            if ratings_reviews_text:
                # Look for patterns like "855 Ratings" and "50 Reviews"
                for text in ratings_reviews_text:
                    clean_text = self.clean_text(text)
                    if 'Ratings' in clean_text:
                        # Extract numbers from ratings text
                        ratings_count = self.extract_reviews_count(clean_text)
                    elif 'Reviews' in clean_text:
                        # Extract numbers from reviews text
                        reviews_count = self.extract_reviews_count(clean_text)
            
            # Alternative method: direct extraction from specific spans
            if not ratings_count:
                ratings_span = response.css('span.Wphh3N span span:nth-child(1)::text').get()
                if ratings_span:
                    ratings_count = self.extract_reviews_count(ratings_span)
            
            if not reviews_count:
                reviews_span = response.css('span.Wphh3N span span:nth-child(3)::text').get()
                if reviews_span:
                    reviews_count = self.extract_reviews_count(reviews_span)
            
            return ratings_count, reviews_count
            
        except Exception as e:
            print(f"Error extracting ratings/reviews: {e}")
            return '', ''
    
    def has_complete_pricing_data(self, item):
        """Check if item has complete pricing data (MRP, Current Price, and Discount)"""
        mrp = item.get('MRP', '').strip()
        current_price = item.get('Current Price', '').strip()
        discount = item.get('Discount', '').strip()
        
        # All three fields must be present and non-empty
        return bool(mrp and current_price and discount)
    
    def preprocess_data(self, item):
        """Preprocess and clean the extracted data with robust price/discount checks"""
        # Clean text fields
        for field in ['Title', 'Brand', 'MRP', 'Current Price', 'Discount', 'Rating', 'Ratings Count', 'Reviews Count']:
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
        item['Ratings Count'] = self.extract_reviews_count(item.get('Ratings Count', ''))
        item['Reviews Count'] = self.extract_reviews_count(item.get('Reviews Count', ''))

        return item
    
    def parse_product(self, response):
        try:
            # Extract basic product information with multiple fallback selectors
            title = None
            
            # Try multiple title selectors
            title_selectors = [
                'span[class*="VU-ZEz"]::text',
                'h1[class*="_6EBuvT"] span::text',
                'h1 span::text',
                'h1::text',
                'span[class*="B_NuCI"]::text'
            ]
            
            for selector in title_selectors:
                title = response.css(selector).get()
                if title and self.clean_text(title):
                    title = self.clean_text(title)
                    break
            
            # Extract brand from title or specifications
            brand = ''
            if title:
                # Try to extract brand from title (first word)
                brand_match = re.match(r'^([\w\-]+)', title)
                if brand_match:
                    brand = brand_match.group(1)
            
            # Extract rating with improved method
            rating = self.extract_rating_properly(response)
            
            # Extract ratings count and reviews count separately
            ratings_count, reviews_count = self.extract_ratings_and_reviews_separately(response)
            
            # Extract current price with multiple selectors
            current_price = ''
            current_price_selectors = [
                'div[class*="Nx9bqj"]::text',
                'div[class*="price"]::text',
                'span[class*="price"]::text',
                'div[class*="_30jeq3"]::text'
            ]
            
            for selector in current_price_selectors:
                price_text = response.css(selector).get()
                if price_text:
                    current_price = self.clean_text(price_text)
                    if current_price:
                        break
            
            # Extract MRP with improved method
            mrp = self.extract_mrp_properly(response)
            
            # Extract discount with multiple selectors
            discount = ''
            discount_selectors = [
                'div[class*="UkUFwK"] span::text',
                'div[class*="discount"]::text',
                'span[class*="discount"]::text',
                'div[class*="_3Ay6Sb"]::text'
            ]
            
            for selector in discount_selectors:
                discount_text = response.css(selector).get()
                if discount_text:
                    discount = self.clean_text(discount_text)
                    if discount:
                        break
            
            # Extract specifications using both methods
            specs = {}
            
            # Try first type of specifications
            specs_type1 = self.parse_specifications_type1(response)
            if specs_type1:
                specs.update(specs_type1)
            
            # If first type didn't find specs, try second type
            if not specs:
                specs_type2 = self.parse_specifications_type2(response)
                if specs_type2:
                    specs.update(specs_type2)
            
            # Extract brand from specifications if not found
            if not brand and 'Brand' in specs:
                brand = specs['Brand']
            
            # Store the product data
            item = {
                'URL': response.url,
                'Title': title or '',
                'Brand': brand,
                'MRP': mrp,
                'Current Price': current_price,
                'Discount': discount,
                'Rating': rating,
                'Ratings Count': ratings_count,
                'Reviews Count': reviews_count,
                'specs': specs
            }
            
            # Preprocess the data
            item = self.preprocess_data(item)
            self.items.append(item)
            
            # Update scraped count and show progress
            self.scraped_count += 1
            print(f"✅ URLs found: {len(self.product_urls)} | Products scraped: {self.scraped_count}", end="\r")
            
            yield item
            
        except Exception as e:
            print(f"❌ Error parsing product: {type(e).__name__}: {e}")
    
    def closed(self, reason):
        # After spider closes, write data to CSV
        self.write_to_csv()
        print(f"\n🎉 Scraping completed! URLs: {len(self.product_urls)}, Products: {self.scraped_count}")
    
    def write_to_csv(self):
        # Constant fields - NOW INCLUDES SEPARATE RATINGS COUNT AND REVIEWS COUNT
        constant_fields = ['URL', 'Title', 'Brand', 'MRP', 'Current Price', 'Discount %', 'Rating', 'Ratings Count', 'Reviews Count']
        placeholder = 'N/A'

        # Step 1: Count non-empty entries for each spec key across all items
        spec_counts = defaultdict(int)
        
        # Filter items to only include those with complete pricing data
        complete_pricing_items = [item for item in self.items if self.has_complete_pricing_data(item)]
        total_items = len(complete_pricing_items)
        
        print(f"📊 Total items: {len(self.items)}, Items with complete pricing: {total_items}")
        
        for item in complete_pricing_items:
            specs = item.get('specs', {})
            for key in self.all_spec_keys:
                value = specs.get(key, '')
                if value and str(value).strip():
                    spec_counts[key] += 1

        # Step 2: Keep only spec keys with at least 40% non-empty data
        threshold = int(0.4 * total_items) if total_items > 0 else 1
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
        filename = os.path.join(data_dir, f'flipkart_{self.query}_products.csv')
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            items_written = 0
            items_skipped = 0
            
            for item in self.items:
                # Skip rows where 'Brand' is missing or empty
                brand_val = item.get('Brand', '')
                if brand_val is None or str(brand_val).strip() == '':
                    items_skipped += 1
                    continue
                
                # NEW: Skip rows without complete pricing data
                if not self.has_complete_pricing_data(item):
                    items_skipped += 1
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
                items_written += 1
            
            print(f"📈 CSV written: {items_written} items written, {items_skipped} items skipped")

if __name__ == "__main__":
    # Configure Scrapy settings
    settings = get_project_settings()
    settings.set('USER_AGENT', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
    settings.set('CONCURRENT_REQUESTS', 100)
    settings.set('CONCURRENT_REQUESTS_PER_DOMAIN', 16)
    settings.set('DOWNLOAD_DELAY', 0)  # Small delay to avoid blocking
    settings.set('AUTOTHROTTLE_ENABLED', False)
    settings.set('RANDOMIZE_DOWNLOAD_DELAY', False)
    settings.set('RETRY_ENABLED', True)
    settings.set('RETRY_TIMES', 2)
    settings.set('COOKIES_ENABLED', False)
    settings.set('DOWNLOAD_TIMEOUT', 15)
    settings.set('LOG_LEVEL', 'ERROR')
    
    # Additional performance optimizations
    settings.set('REACTOR_THREADPOOL_MAXSIZE', 20)
    settings.set('DNS_TIMEOUT', 10)
    settings.set('DOWNLOAD_MAXSIZE', 0)
    settings.set('REDIRECT_ENABLED', True)
    settings.set('REDIRECT_MAX_TIMES', 2)
    settings.set('AJAXCRAWL_ENABLED', False)
    settings.set('TELNETCONSOLE_ENABLED', False)
    
    # Get user input
    search_query = input("Enter search query (e.g., 'laptop'): ").strip()
    num_pages = input("Enter number of pages to scrape (e.g., 5): ").strip()
    if not num_pages.isdigit() or int(num_pages) < 1:
        print("No valid page number entered. Exiting without scraping.")
    else:
        num_pages = int(num_pages)
        process = CrawlerProcess(settings)
        process.crawl(FlipkartSpider, query=search_query, pages=num_pages)
        process.start()