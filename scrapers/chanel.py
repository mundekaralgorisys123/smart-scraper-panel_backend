import asyncio
import base64
import os
import uuid
import logging
from datetime import datetime
from bs4 import BeautifulSoup
import re
from typing import Dict, Any, List
import requests
from urllib.parse import urlparse
from openpyxl import Workbook
from database_quey.db_inseartin import insert_into_db, update_product_count

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

IMAGE_SAVE_PATH = os.getenv("IMAGE_SAVE_PATH")
EXCEL_DATA_PATH = os.getenv("EXCEL_DATA_PATH")


class ChanelScraper:
    """Scraper for Chanel product pages with database and Excel functionality"""
    
    def __init__(self, excel_data_path=EXCEL_DATA_PATH, image_save_path=IMAGE_SAVE_PATH):
        self.excel_data_path = excel_data_path
        self.image_save_path = image_save_path
        self.setup_directories()
    
    def setup_directories(self):
        """Create necessary directories"""
        os.makedirs(self.excel_data_path, exist_ok=True)
        os.makedirs(self.image_save_path, exist_ok=True)
    
    def parse_and_save_products(self, products_data: List[Dict], page_title: str, page_url: str = "") -> Dict[str, Any]:
        """
        Main method to parse products and save to database/Excel
        Returns: JSON response compatible with your requirements
        """
        try:
            print("=================== Starting Chanel Scraper ==================")
            print(f"Processing {len(products_data)} product entries")
            
            # Extract HTML content
            html_content = products_data[0].get('html', '') if products_data else ''
            
            # Parse individual products from HTML
            individual_products = self.extract_individual_products_from_html(html_content)
            print(f"Extracted {len(individual_products)} individual products")
            
            # Generate unique session ID and timestamp
            session_id = str(uuid.uuid4())
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            current_date = datetime.now().date()
            current_time = datetime.now().time()
            
            # Create image folder for this session
            image_folder = os.path.join(self.image_save_path, f"chanel_{timestamp}")
            os.makedirs(image_folder, exist_ok=True)
            
            # Create Excel file
            excel_filename = f"chanel_scraped_products_{timestamp}.xlsx"
            excel_path = os.path.join(self.excel_data_path, excel_filename)
            
            # Process products
            database_records = []
            successful_downloads = 0
            
            # Create Excel workbook
            wb = Workbook()
            sheet = wb.active
            sheet.title = "Chanel Products"
            
            # Add headers
            headers = [
                'Unique ID', 'Current Date', 'Page Title', 'Product Name', 
                'Image Path', 'Gold Type', 'Price', 'Diamond Weight', 
                'Additional Info', 'Scrape Time', 'Image URL', 'Product Link',
                'Session ID', 'Page URL'
            ]
            sheet.append(headers)
            
            # Process each product
            for i, product_html in enumerate(individual_products):
                try:
                    # Parse product data
                    parsed_data = self.parse_product(product_html)
                    
                    # Generate unique ID
                    unique_id = str(uuid.uuid4())
                    product_name = parsed_data.get('product_name', 'Unknown Product')[:495]
                    
                    # Download image - use sync method
                    image_url = parsed_data.get('image_url')
                    image_path = self.download_image(
                        image_url, product_name, timestamp, image_folder, unique_id
                    )
                    
                    if image_path != "N/A":
                        successful_downloads += 1
                    
                    # Prepare additional info
                    badges = parsed_data.get('badges', [])
                    promotions = parsed_data.get('promotions', '')
                    additional_info_parts = []
                    
                    if badges:
                        additional_info_parts.extend(badges)
                    if promotions and promotions != "N/A":
                        additional_info_parts.append(promotions)
                    
                    additional_info = " | ".join(additional_info_parts) if additional_info_parts else "N/A"
                    
                    # Create database record
                    db_record = {
                        'unique_id': unique_id,
                        'current_date': current_date,
                        'page_title': page_title,
                        'product_name': product_name,
                        'image_path': image_path,
                        'price': parsed_data.get('price'),
                        'diamond_weight': parsed_data.get('diamond_weight'),
                        'gold_type': parsed_data.get('gold_type'),
                        'additional_info': additional_info,
                    }
                    
                    database_records.append(db_record)
                    
                    # Add to Excel
                    sheet.append([
                        unique_id,
                        current_date.strftime('%Y-%m-%d'),
                        page_title,
                        product_name,
                        image_path,
                        parsed_data.get('gold_type', 'N/A'),
                        parsed_data.get('price', 'N/A'),
                        parsed_data.get('diamond_weight', 'N/A'),
                        additional_info,
                        current_time.strftime('%H:%M:%S'),
                        image_url,
                        parsed_data.get('link', 'N/A'),
                        session_id,
                        page_url
                    ])
                    
                    print(f"Processed product {i+1}: {product_name}")
                    
                except Exception as e:
                    print(f"Error processing product {i}: {e}")
                    continue
            
            # Save Excel file
            wb.save(excel_path)
            print(f"Excel file saved: {excel_path}")
            
            # Insert data into the database and update product count
            insert_into_db(database_records)
            update_product_count(len(database_records))
            
            # Encode Excel file to base64
            with open(excel_path, "rb") as file:
                base64_file = base64.b64encode(file.read()).decode("utf-8")
            
            # Return JSON response
            return {
                'message': f'Successfully processed {len(database_records)} products',
                'session_id': session_id,
                'excel_file': excel_filename,
                'total_processed': len(database_records),
                'images_downloaded': successful_downloads,
                'failed': len(individual_products) - len(database_records),
                'website_type': 'chanel',
                'base64_file': base64_file,
                'file_path': excel_path
            }
            
        except Exception as e:
            print(f"Error in parse_and_save_products: {e}")
            return {
                'error': str(e),
                'message': 'Failed to process products'
            }
    
    def parse_product(self, product_html: str) -> Dict[str, Any]:
        """Parse individual product HTML using Chanel specific structure"""
        soup = BeautifulSoup(product_html, 'html.parser')
        
        return {
            'product_name': self._extract_product_name(soup),
            'price': self._extract_price(soup),
            'image_url': self._extract_image(soup),
            'link': self._extract_link(soup),
            'diamond_weight': self._extract_diamond_weight(soup),
            'gold_type': self._extract_gold_type(soup),
            'badges': self._extract_badges(soup),
            'promotions': self._extract_promotions(soup)
        }
    
    def extract_individual_products_from_html(self, html_content: str) -> List[str]:
        """Extract individual product HTML blocks from Chanel HTML"""
        if not html_content:
            return []
        
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Chanel specific product selectors
        product_selectors = [
            'div.product-grid__item.js-product-edito',  # Main product grid item
            'article.product',  # Product article
            '.product-grid__item',  # Product grid item
            '[data-id]',  # Items with data-id
        ]
        
        individual_products = []
        
        for selector in product_selectors:
            product_tiles = soup.select(selector)
            for tile in product_tiles:
                individual_products.append(str(tile))
            if individual_products:
                break
        
        print(f"Found {len(individual_products)} product tiles in Chanel HTML")
        return individual_products
    
    def _extract_product_name(self, soup) -> str:
        """Extract product name from Chanel product tile by combining name and description"""
        # Extract main product name
        name_selectors = [
            'span[data-test="lnkProductPLP_BySKU"]',  # Product name span
            '[data-product-element="name"]',  # Data attribute
            '.txt-product__title',  # Product title
            'span.heading.is-7',  # Heading span
        ]
        
        main_name = "N/A"
        for selector in name_selectors:
            name_element = soup.select_one(selector)
            if name_element:
                main_name = self.clean_text(name_element.get_text())
                break
        
        # Extract description to get gold type and materials
        desc_selectors = [
            'span[data-test="lblProductShrotDescription_PLP"]',  # Description span
            '[data-product-element="description"]',  # Data attribute
            '.js-ellipsis',  # Ellipsis class
        ]
        
        description = "N/A"
        for selector in desc_selectors:
            desc_element = soup.select_one(selector)
            if desc_element:
                description = self.clean_text(desc_element.get_text())
                break
        
        # Combine name and description to create full product name
        if main_name != "N/A" and description != "N/A":
            # Format: "Eternal N°5 ring - 18K white gold, diamonds"
            full_name = f"{main_name} - {description}"
        elif main_name != "N/A":
            full_name = main_name
        elif description != "N/A":
            full_name = description
        else:
            full_name = "Unknown Product"
        
        return full_name[:495]  # Truncate to fit database field
    
    def _extract_price(self, soup) -> str:
        """Extract price from Chanel product"""
        price_selectors = [
            'p[data-test="lblProductPrice_PLP"]',  # Price paragraph
            '.is-price',  # Price class
            '[data-product-element="price"]',  # Data attribute
            'p.is-price',  # Price paragraph with class
        ]
        
        for selector in price_selectors:
            price_element = soup.select_one(selector)
            if price_element:
                price_text = price_element.get_text(strip=True)
                # Remove the * and disclaimer text
                price_text = re.sub(r'\*.*', '', price_text)
                return self.extract_price_value(price_text)
        
        return "N/A"
    
    def _extract_image(self, soup) -> str:
        """Extract image URL from Chanel product with multiple fallbacks"""
        # Chanel specific image selectors and attributes
        img_selectors = [
            'img',  # Any image
            '.product__media img',  # Product media image
            'img.lazyautosizes',  # Lazy loaded images
        ]
        
        for selector in img_selectors:
            img_element = soup.select_one(selector)
            if img_element:
                # print(f"Found image element with selector: {selector}")
                
                # Try multiple attribute sources in priority order
                attributes_to_try = ['data-src', 'src', 'data-srcset']
                
                for attr in attributes_to_try:
                    image_value = img_element.get(attr)
                    if image_value:
                        # print(f"Found image in {attr}: {image_value}")
                        processed_url = self._process_image_url(image_value, attr)
                        if processed_url and processed_url != "N/A":
                            # print(f"Processed URL: {processed_url}")
                            return processed_url
        
        print("No valid image URL found")
        return "N/A"
    
    def _process_image_url(self, image_value: str, source_attr: str = "") -> str:
        """Process image URL with multiple fallbacks and normalization"""
        if not image_value or image_value == "N/A":
            return "N/A"
        
        # print(f"Processing image from {source_attr}: {image_value}")
        
        # Handle srcset (comma-separated URLs with descriptors)
        if source_attr in ['srcset', 'data-srcset'] and ',' in image_value:
            print("Processing srcset...")
            # Split by comma and get all URLs
            urls = []
            for url_part in image_value.split(','):
                url_part = url_part.strip()
                # Split by space and take the first part (the URL)
                url = url_part.split()[0] if url_part.split() else url_part
                if url and 'chanel.com' in url:
                    urls.append(url)
            
            if urls:
                # Get the highest resolution image (usually the one with w_1920 or the last one)
                high_res_url = None
                for url in urls:
                    if '/w_1920//' in url:
                        high_res_url = url
                        break
                
                if not high_res_url:
                    # If no w_1920 found, take the last one (usually highest resolution)
                    high_res_url = urls[-1]
                
                print(f"Selected from srcset: {high_res_url}")
                return self._normalize_image_url(high_res_url)
        
        # For single URLs, just normalize them
        return self._normalize_image_url(image_value)
    
    def _normalize_image_url(self, url: str) -> str:
        """Normalize image URL for Chanel - ensure it's complete and high quality"""
        if not url or url == "N/A":
            return "N/A"
        
        # print(f"Normalizing URL: {url}")
        
        # Ensure URL is complete
        if url.startswith('//'):
            url = f"https:{url}"
            print(f"Added protocol: {url}")
        elif url.startswith('/') and 'chanel.com' not in url:
            url = f"https://www.chanel.com{url}"
            print(f"Added domain: {url}")
        
        # If it's still not a complete URL, try to reconstruct it
        if not url.startswith('http') and 'chanel.com' not in url:
            print(f"Incomplete URL detected: {url}")
            # Extract the core filename if possible
            filename_match = re.search(r'([a-zA-Z0-9\-]+\.jpg)', url)
            if filename_match:
                core_filename = filename_match.group(1)
                url = f"https://www.chanel.com/images/t_one////q_auto:best,f_auto,fl_lossy,dpr_1.1/w_1920//{core_filename}"
                print(f"Reconstructed URL: {url}")
            else:
                return "N/A"
        
        # Now enhance quality for complete URLs
        if 'chanel.com' in url:
            # Ensure we're using the highest resolution
            if '/w_1920//' not in url:
                url = re.sub(r'/w_\d+//', '/w_1920//', url)
                # print(f"Enhanced resolution: {url}")
            
            # Improve quality parameters
            if 'q_auto:good' in url:
                url = url.replace('q_auto:good', 'q_auto:best')
                # print(f"Enhanced quality: {url}")
        
        # print(f"Final normalized URL: {url}")
        return url
    
    def _extract_link(self, soup) -> str:
        """Extract product link from Chanel product"""
        link_selectors = [
            'a[data-test="product_link"]',  # Product link
            'a[data-product-element="image"]',  # Image link
            '.txt-product a',  # Text product link
            'a[href*="/p/"]',  # Links with product path
        ]
        
        for selector in link_selectors:
            link_element = soup.select_one(selector)
            if link_element and link_element.get('href'):
                href = link_element.get('href')
                return self._normalize_link_url(href)
        
        return "N/A"
    
    def _extract_diamond_weight(self, soup) -> str:
        """Extract diamond weight from product description"""
        description = self._extract_description(soup)
        return self.extract_diamond_weight_value(description)
    
    def _extract_description(self, soup) -> str:
        """Extract product description"""
        desc_selectors = [
            'span[data-test="lblProductShrotDescription_PLP"]',  # Description span
            '[data-product-element="description"]',  # Data attribute
            '.js-ellipsis',  # Ellipsis class
        ]
        
        for selector in desc_selectors:
            desc_element = soup.select_one(selector)
            if desc_element:
                return self.clean_text(desc_element.get_text())
        
        return "N/A"
    
    def _extract_gold_type(self, soup) -> str:
        """Extract gold type from product description"""
        description = self._extract_description(soup)
        gold_type = self.extract_gold_type_value(description)
        
        if gold_type != "N/A":
            return gold_type
        
        # Also check product name for gold type
        product_name = self._extract_product_name(soup)
        return self.extract_gold_type_value(product_name)
    
    def _extract_badges(self, soup) -> list:
        """Extract badges and additional info from Chanel product"""
        badges = []
        
        # Extract from flag elements (matching your Playwright code)
        badge_selectors = [
            'p.flag.is-1',  # Flag badges
            '.flag',  # Flag class
            '[class*="badge"]',  # Any badge class
            '[class*="tag"]',  # Any tag class
        ]
        
        for selector in badge_selectors:
            badge_elements = soup.select(selector)
            for badge in badge_elements:
                badge_text = self.clean_text(badge.get_text())
                if badge_text and badge_text not in badges:
                    badges.append(badge_text)
        
        # Extract collection from data attribute
        product_element = soup.select_one('article.product')
        if product_element and product_element.get('data-collection'):
            collection = product_element.get('data-collection')
            if collection and collection != "N/A":
                badges.append(f"Collection: {collection}")
        
        return badges
    
    def _extract_promotions(self, soup) -> str:
        """Extract promotion text from Chanel product"""
        # Look for promotional elements
        promo_selectors = [
            '.disclaimer-indicator',  # Price disclaimer
            '[class*="promo"]',  # Any promo class
            '[class*="sale"]',  # Any sale class
            '[class*="new"]',  # Any new class
        ]
        
        for selector in promo_selectors:
            promo_elements = soup.select(selector)
            promo_texts = []
            for promo in promo_elements:
                promo_text = self.clean_text(promo.get_text())
                if promo_text and ("sale" in promo_text.lower() or "promo" in promo_text.lower() or "new" in promo_text.lower()):
                    promo_texts.append(promo_text)
            
            if promo_texts:
                return " | ".join(promo_texts)
        
        return "N/A"
    
    def _normalize_link_url(self, url: str) -> str:
        """Normalize link URL for Chanel"""
        if not url or url == "N/A":
            return "N/A"
        if url.startswith('http'):
            return url
        elif url.startswith('//'):
            return f"https:{url}"
        elif url.startswith('/'):
            return f"https://www.chanel.com{url}"
        return url

    def modify_image_url(self, image_url: str) -> str:
        """Modify URLs only for Chanel; return as-is for all other brands"""

        if not image_url or image_url == "N/A":
            return image_url

        # Do NOT touch Van Cleef & Arpels URLs
        if "vancleefarpels.com" in image_url:
            return image_url

        # Chanel logic
        if image_url.startswith('https://') and '/w_1920//' in image_url:
            return image_url

        return self._normalize_image_url(image_url)


    def download_image(self, image_url: str, product_name: str, timestamp: str, 
                      image_folder: str, unique_id: str, retries: int = 3) -> str:
        """Synchronous image download method with enhanced error handling"""
        if not image_url or image_url == "N/A":
            return "N/A"

        # Clean filename
        image_filename = f"{unique_id}_{timestamp}.jpg"
        image_full_path = os.path.join(image_folder, image_filename)
        
        # Modify the image URL to get higher quality
        modified_url = self.modify_image_url(image_url)
        # print(f"Downloading image for {product_name} from {modified_url}")
        
        for attempt in range(retries):
            try:
                response = requests.get(
                    modified_url,
                    timeout=45,
                    headers={
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128 Safari/537.36",
                        "Referer": "https://www.chanel.com/",
                        "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
                        "Accept-Language": "en-US,en;q=0.9",
                        "Connection": "keep-alive"
                    },
                    allow_redirects=True,
                    stream=True
                )
                response.raise_for_status()
                
                # Verify it's actually an image
                content_type = response.headers.get('content-type', '')
                if not content_type.startswith('image/'):
                    logger.warning(f"URL {modified_url} returned non-image content type: {content_type}")
                    continue
                    
                with open(image_full_path, "wb") as f:
                    f.write(response.content)
                
                logger.info(f"Successfully downloaded image for {product_name}")
                return image_full_path
                
            except requests.RequestException as e:
                logger.warning(f"Retry {attempt + 1}/{retries} - Error downloading {product_name}: {e}")
                if attempt < retries - 1:
                    import time
                    time.sleep(2)
        
        logger.error(f"Failed to download {product_name} after {retries} attempts.")
        return "N/A"
    
    def extract_price_value(self, text: str) -> str:
        """Extract price from text"""
        if not text:
            return "N/A"
        
        # Chanel price formats: "₹ 902,100", "€ 1,200", "$ 1,500"
        price_match = re.search(r'[₹€$]\s*\d{1,3}(?:,\d{3})*(?:\.\d{2})?', text)
        if price_match:
            return price_match.group(0).replace(' ', '')  # Remove spaces
        
        return "N/A"
    
    def clean_text(self, text: str) -> str:
        """Clean and normalize text"""
        if not text:
            return ""
        text = ' '.join(text.split()).strip()
        text = re.sub(r'\s+', ' ', text)
        return text
    
    def extract_diamond_weight_value(self, text: str) -> str:
        """Extract diamond weight from text"""
        if not text:
            return "N/A"
        
        weight_patterns = [
            r'(\d+(?:\.\d+)?)\s*ct\s*tw',
            r'(\d+(?:\.\d+)?)\s*ctw',
            r'(\d+(?:\.\d+)?)\s*carat',
            r'(\d+(?:\.\d+)?)\s*carats',
        ]
        
        for pattern in weight_patterns:
            weight_match = re.search(pattern, text, re.IGNORECASE)
            if weight_match:
                weight = weight_match.group(1)
                if 'tw' not in text.lower():
                    return f"{weight} ct tw"
                return f"{weight} ct"
        
        return "N/A"
    
    def extract_gold_type_value(self, text: str) -> str:
        """Extract gold type from text"""
        if not text:
            return "N/A"
        
        gold_patterns = [
            r'(\d+k)\s*(?:White|Yellow|Rose|BEIGE)?\s*Gold',
            r'(White|Yellow|Rose|BEIGE)\s*Gold\s*(\d+k)',
            r'(\d+k)\s*Gold',
            r'(Platinum|Sterling Silver|Silver)',
            r'(White Gold|Yellow Gold|Rose Gold|BEIGE GOLD)',
        ]
        
        for pattern in gold_patterns:
            gold_match = re.search(pattern, text, re.IGNORECASE)
            if gold_match:
                gold_parts = [part for part in gold_match.groups() if part]
                return ' '.join(gold_parts).title()
        
        return "N/A"