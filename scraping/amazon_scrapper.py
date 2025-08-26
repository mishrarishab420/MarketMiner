import os
import re
import sys
import pandas as pd
from urllib.parse import quote_plus
from bs4 import BeautifulSoup
from scraping.base_scrapper import BaseScraper

class AmazonScraper(BaseScraper):
    BASE_URL = "https://www.amazon.in/s?k={query}&page={page}"

    def __init__(self, query, max_pages=1):
        super().__init__(base_url=self.BASE_URL)
        self.query = query
        self.max_pages = max_pages

    def extract_brand_from_title(self, title):
        """Extract brand from title using simple heuristic."""
        if not title:
            return None
        words = title.split()
        if len(words) >= 1:
            brand = " ".join(words[:1])  # take first word
            return brand.capitalize()
        return words[0].capitalize()

    def scrape(self):
        results = []
        for page in range(1, self.max_pages + 1):
            url = self.BASE_URL.format(query=quote_plus(self.query), page=page)
            html = self.get_page_with_retry(url)
            if not html:
                continue
            soup = BeautifulSoup(html, "html.parser")
            product_divs = soup.select("div[data-component-type='s-search-result']")
            print(f"Found {len(product_divs)} products on page {page}")

            for div in product_divs:
                # Title
                title_elem = (
                    div.select_one("h2.a-size-mini.a-spacing-none.a-color-base.s-line-clamp-2 a span")
                    or div.select_one("h2.a-size-medium.a-text-normal")
                    or div.select_one("span.a-text-normal")
                )

                # Try to get nested span if present (to avoid "Sponsored Ad - ..." prefix)
                if title_elem:
                    # If the element has a span inside, use that text instead
                    nested_span = title_elem.find("span")
                    if nested_span and nested_span.text.strip():
                        title = self.clean_text(nested_span.text)
                    else:
                        title = self.clean_text(title_elem.text)
                else:
                    # Fallback: use image alt attribute if title not found
                    img_alt_elem = div.select_one("img.s-image")
                    if img_alt_elem and img_alt_elem.get("alt"):
                        title = self.clean_text(img_alt_elem.get("alt"))
                    else:
                        title = None

                if not title:
                    print("⚠️ Skipped product, no title found")

                # Brand (try element, fallback to title heuristic)
                brand_elem = div.select_one("h5.s-line-clamp-1 span.a-size-base-plus.a-color-base")
                if brand_elem:
                    brand = self.clean_text(brand_elem.text).capitalize()
                else:
                    brand = self.extract_brand_from_title(title)

                # Updated brand replacement logic
                if brand and "sponsored" in brand.lower():
                    if title:
                        brand = title.split()[0].capitalize()

                # Price
                current_price_elem = div.select_one("span.a-price span.a-offscreen")
                current_price = self.extract_price(current_price_elem.text) if current_price_elem else None

                # MRP
                mrp_elem = div.select_one("span.a-text-price span.a-offscreen")
                mrp = self.extract_price(mrp_elem.text) if mrp_elem else None

                if mrp is None and current_price is not None:
                    mrp = current_price

                discount = None
                if current_price and mrp and mrp > 0:
                    try:
                        discount = int(round((mrp - current_price) / mrp * 100))
                    except Exception:
                        discount = None
                if discount is None:
                    discount = 0

                # Rating
                rating_elem = div.select_one("span.a-icon-alt")
                rating = None
                if rating_elem:
                    try:
                        rating_value = rating_elem.text.split()[0]
                        rating = float(rating_value)
                    except Exception:
                        rating = None

                # Reviews
                reviews_elem = div.select_one("span.a-size-base.s-underline-text")
                reviews = self.clean_text(reviews_elem.text) if reviews_elem else None

                # Autofill missing rating/reviews
                if reviews is None and rating is not None:
                    reviews = "0"
                if rating is None and reviews is not None:
                    rating = 0
                if rating is None and reviews is None:
                    rating = 0
                    reviews = "0"

                # Image
                img_elem = div.select_one("img.s-image")
                image = img_elem["src"] if img_elem else None

                # Link
                # Try h2 a, fallback to a.a-link-normal (both title and image block)
                link_elem = div.select_one("h2 a")
                if not link_elem:
                    # try any a.a-link-normal (image or title block)
                    link_elem = div.select_one("a.a-link-normal")
                link = None
                if link_elem and link_elem.has_attr("href"):
                    href = link_elem["href"]
                    # Properly decode and extract /dp/... in case of redirect URLs
                    # Example: /gp/slredirect/picassoRedirect.html/ref=...&url=%2Fdp%2FB09V4MXBSN%2F...
                    # We want to extract /dp/B09V4MXBSN/
                    import urllib.parse
                    # Check for url= param in href
                    match = re.search(r'[?&]url=([^&]+)', href)
                    dp_path = None
                    if match:
                        url_param = urllib.parse.unquote(match.group(1))
                        dp_match = re.search(r'(/dp/[\w\d]+)/?', url_param)
                        if dp_match:
                            dp_path = dp_match.group(1)
                    if not dp_path:
                        # Try to extract /dp/ID directly from href
                        dp_match = re.search(r'(/dp/[\w\d]+)/?', href)
                        if dp_match:
                            dp_path = dp_match.group(1)
                    if dp_path:
                        link = "https://www.amazon.in" + dp_path
                    else:
                        # fallback: prepend domain if not absolute
                        if href.startswith("http"):
                            link = href
                        else:
                            link = "https://www.amazon.in" + href

                # Filter out 'More like this' and placeholder image items
                if title and title == "More like this":
                    print("⚠️ Skipped 'More like this' item")
                    continue
                if image and "grey-pixel.gif" in image:
                    print("⚠️ Skipped placeholder image item")
                    continue
                # Clean sponsored ad prefix but keep the product
                if title and title.startswith("Sponsored Ad - "):
                    title = title[len("Sponsored Ad - "):].strip()
                # Ensure no field is left as None in the result dict
                if title:
                    results.append({
                        "Brand": brand if brand is not None else "",
                        "Title": title if title is not None else "",
                        "Current Price": current_price if current_price is not None else 0,
                        "MRP": mrp if mrp is not None else 0,
                        "Discount (%)": discount if discount is not None else 0,
                        "Rating": rating if rating is not None else 0,
                        "Reviews": reviews if reviews is not None else "0",
                        "URL": link if link is not None else "",
                    })
            self.random_delay()
        df = self.save_to_dataframe(results)
        return df


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Amazon Scraper Standalone")
    parser.add_argument("query", type=str, help="Search query")
    parser.add_argument("--max_pages", type=int, default=1, help="Number of pages to scrape")
    args = parser.parse_args()
    scraper = AmazonScraper(args.query, max_pages=args.max_pages)
    df = scraper.scrape()
    out_dir = os.path.join("data", "final")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"amazon_{args.query.replace(' ', '_')}.csv")
    df.to_csv(out_path, index=False)
    print(f"Saved {len(df)} results to {out_path}")