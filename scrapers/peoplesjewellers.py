import os
import uuid
import base64
import logging
import requests
from datetime import datetime
from bs4 import BeautifulSoup
import re
from typing import Dict, Any, List
from openpyxl import Workbook
from database_quey.db_inseartin import insert_into_db, update_product_count

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

IMAGE_SAVE_PATH = os.getenv("IMAGE_SAVE_PATH")
EXCEL_DATA_PATH = os.getenv("EXCEL_DATA_PATH")


class PeoplesJewellersParser:
    """Parser for People's Jewellers product pages"""
    
    def __init__(self, excel_data_path=EXCEL_DATA_PATH, image_save_path=IMAGE_SAVE_PATH):
        self.excel_data_path = excel_data_path
        self.image_save_path = image_save_path
        self.setup_directories()
        self.downloaded_images = set()  # Track downloaded images to avoid duplicates
        self.processed_products = set()  # Track processed products to avoid duplicates
    
    def setup_directories(self):
        """Create necessary directories"""
        os.makedirs(self.excel_data_path, exist_ok=True)
        os.makedirs(self.image_save_path, exist_ok=True)
    
    def parse_and_save_products(self, products_data: List[Dict], page_title: str, page_url: str = "") -> Dict[str, Any]:
        """
        Main method to parse products and save to database/Excel
        """
        try:
            print("=================== Starting People's Jewellers Parser ==================")
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
            image_folder = os.path.join(self.image_save_path, f"peoples_{timestamp}")
            os.makedirs(image_folder, exist_ok=True)
            
            # Create Excel file
            excel_filename = f"peoples_scraped_products_{timestamp}.xlsx"
            excel_path = os.path.join(self.excel_data_path, excel_filename)
            
            # Process products
            database_records = []
            successful_downloads = 0
            
            # Create Excel workbook
            wb = Workbook()
            sheet = wb.active
            sheet.title = "Peoples Products"
            
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
                    
                    # Skip if we've already processed this product (based on product name and image URL)
                    product_name = parsed_data.get('product_name', 'Unknown Product')
                    image_url = parsed_data.get('image_url')
                    product_key = f"{product_name}_{image_url}"
                    
                    if product_key in self.processed_products:
                        print(f"Skipping duplicate product: {product_name}")
                        continue
                    
                    self.processed_products.add(product_key)
                    
                    # Generate unique ID
                    unique_id = str(uuid.uuid4())
                    product_name = product_name[:495]
                    
                    # Download image - check if we've already downloaded this image
                    if image_url in self.downloaded_images:
                        print(f"Skipping duplicate image: {image_url}")
                        image_path = "N/A"
                    else:
                        image_path = self.download_image(
                            image_url, product_name, timestamp, image_folder, unique_id
                        )
                        if image_path != "N/A":
                            successful_downloads += 1
                            self.downloaded_images.add(image_url)  # Track downloaded image
                    
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
                'website_type': 'peoples_jewellers',
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
        """Parse individual product HTML"""
        soup = BeautifulSoup(product_html, 'html.parser')
        
        product_data = {
            'product_name': self._extract_product_name(soup),
            'price': self._extract_price(soup),
            'image_url': self._extract_image(soup),
            'link': self._extract_link(soup),
            'diamond_weight': self._extract_diamond_weight(soup),
            'gold_type': self._extract_gold_type(soup),
            'badges': self._extract_badges(soup),
            'promotions': self._extract_promotions(soup)
        }
        
        print(f"Extracted product data: {product_data['product_name']}, Image: {product_data['image_url']}")
        return product_data
    
    def extract_individual_products_from_html(self, html_content: str) -> List[str]:
        """Extract individual product HTML blocks from People's Jewellers HTML"""
        if not html_content:
            return []
        
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # People's Jewellers product selectors - more specific
        product_selectors = [
            'app-product-grid-item',  # Angular component
            'div.product-grid_tile',  # Product tile
            'div.product-item',       # Product item
            'div[data-product-id]',   # Products with data attributes
        ]
        
        individual_products = []
        found_product_ids = set()  # Track product IDs to avoid duplicates
        
        for selector in product_selectors:
            product_tiles = soup.select(selector)
            for tile in product_tiles:
                # Get product ID to check for duplicates
                product_id = tile.get('data-product-id') or tile.get('data-unique-sigunbxd-id')
                
                # Check if this looks like a product (has image, name, or price)
                tile_html = str(tile)
                tile_soup = BeautifulSoup(tile_html, 'html.parser')
                
                # Verify it's a product by checking for key elements
                has_name = bool(tile_soup.select_one('h2.name, .product-tile-description, [itemprop="url"]'))
                has_image = bool(tile_soup.select_one('img[src*="productimages"], img[itemprop="image"]'))
                has_price = bool(tile_soup.select_one('.price, .product-prices, .current-price'))
                
                if has_name or has_image or has_price:
                    # Use product ID for deduplication, or use the entire HTML as fallback
                    unique_key = product_id if product_id else tile_html
                    
                    if unique_key not in found_product_ids:
                        individual_products.append(tile_html)
                        found_product_ids.add(unique_key)
                        print(f"Added product with ID: {product_id}")
        
        print(f"Found {len(individual_products)} unique product tiles after deduplication")
        return individual_products
        
    def _extract_product_name(self, soup) -> str:
        """Extract product name from product tile"""
        name_selectors = [
            'h2.name a',
            '.product-tile-description a',
            'a[itemprop="url"]',
            '.name a'
        ]
        
        for selector in name_selectors:
            name_element = soup.select_one(selector)
            if name_element and name_element.get_text(strip=True):
                return self.clean_text(name_element.get_text())
        
        return "N/A"
    
    def _extract_price(self, soup) -> str:
        """Extract price information"""
        price_selectors = [
            '.price .plp-align',
            '.product-prices .price',
            '.current-price',
            '.sales-price'
        ]
        
        for selector in price_selectors:
            price_element = soup.select_one(selector)
            if price_element:
                price_text = price_element.get_text(strip=True)
                extracted_price = self.extract_price_value(price_text)
                if extracted_price != "N/A":
                    return extracted_price
        
        # Look for price in any text
        price_match = re.search(r'\$\d{1,3}(?:,\d{3})*(?:\.\d{2})?', soup.get_text())
        if price_match:
            return price_match.group(0)
        
        return "N/A"
    
    def _extract_image(self, soup) -> str:
        """Extract product image URL from People's Jewellers product"""
        # More specific selectors to target only product images
        img_selectors = [
            'app-product-primary-image img[itemprop="image"]',  # Primary product image
            'cx-generic-link img[itemprop="image"]',  # Generic link with product image
            '.main-thumb img[itemprop="image"]',  # Main thumbnail with schema
            'img[itemprop="image"][src*="productimages"]',  # Schema image with productimages
            'img.plpimage[src*="productimages"]',  # PLP image
        ]
        
        for selector in img_selectors:
            img_element = soup.select_one(selector)
            if img_element and img_element.get('src'):
                src = img_element.get('src')
                if src and src != "N/A":
                    # Skip badge images and other non-product images
                    if any(bad_word in src.lower() for bad_word in ['badge', 'medias', 'placeholder']):
                        continue
                    normalized_url = self._normalize_image_url(src)
                    if normalized_url != "N/A":
                        return normalized_url
        
        # Fallback: Look for any image in productimages that's not a badge
        all_images = soup.find_all('img', src=re.compile(r'productimages'))
        for img in all_images:
            src = img.get('src')
            if src and src != "N/A":
                # Skip badge images and other non-product images
                if any(bad_word in src.lower() for bad_word in ['badge', 'medias', 'placeholder']):
                    continue
                normalized_url = self._normalize_image_url(src)
                if normalized_url != "N/A":
                    return normalized_url
        
        return "N/A"
    
    def _extract_link(self, soup) -> str:
        """Extract product link"""
        link_selectors = [
            'h2.name a',
            '.main-thumb a',
            'a[itemprop="url"]',
            '.name a'
        ]
        
        for selector in link_selectors:
            link_element = soup.select_one(selector)
            if link_element and link_element.get('href'):
                href = link_element.get('href')
                return self._normalize_link_url(href)
        
        return "N/A"
    
    def _extract_diamond_weight(self, soup) -> str:
        """Extract diamond weight from product name"""
        product_name = self._extract_product_name(soup)
        return self.extract_diamond_weight_value(product_name)
    
    def _extract_gold_type(self, soup) -> str:
        """Extract gold type from product name"""
        product_name = self._extract_product_name(soup)
        return self.extract_gold_type_value(product_name)
    
    def _extract_badges(self, soup) -> list:
        """Extract badge information"""
        badges = []
        
        badge_selectors = [
            '.badge-container span',
            '.tag-text',
            '.product-tag',
            '.badge'
        ]
        
        for selector in badge_selectors:
            badge_elements = soup.select(selector)
            for badge in badge_elements:
                badge_text = self.clean_text(badge.get_text())
                if badge_text and badge_text not in badges:
                    badges.append(badge_text)
        
        return badges
    
    def _extract_promotions(self, soup) -> str:
        """Extract promotion text"""
        promo_selectors = [
            '.tag-text',
            '.discount-percentage',
            '.promo-text',
            '.sale-text'
        ]
        
        for selector in promo_selectors:
            promo_elements = soup.select(selector)
            promo_texts = []
            for promo in promo_elements:
                promo_text = self.clean_text(promo.get_text())
                if promo_text and ("off" in promo_text.lower() or "sale" in promo_text.lower()):
                    promo_texts.append(promo_text)
            
            if promo_texts:
                return " | ".join(promo_texts)
        
        return "N/A"
    
    def _normalize_image_url(self, url: str) -> str:
        """Normalize image URL"""
        if not url:
            return "N/A"
        if url.startswith('http'):
            return url
        elif url.startswith('//'):
            return f"https:{url}"
        elif url.startswith('/'):
            return f"https://www.peoplesjewellers.com{url}"
        return url
    
    def _normalize_link_url(self, url: str) -> str:
        """Normalize link URL"""
        if not url:
            return "N/A"
        if url.startswith('http'):
            return url
        elif url.startswith('//'):
            return f"https:{url}"
        elif url.startswith('/'):
            return f"https://www.peoplesjewellers.com{url}"
        return url

    def modify_image_url(self, image_url: str) -> str:
        """Modify the image URL to replace '_260' with '_1200' while keeping query parameters."""
        if not image_url or image_url == "N/A":
            return image_url

        # Extract and preserve query parameters
        query_params = ""
        if "?" in image_url:
            image_url, query_params = image_url.split("?", 1)
            query_params = f"?{query_params}"

        # Replace '_260' with '_1200' while keeping the rest of the URL intact
        modified_url = re.sub(r'(_260)(?=\.\w+$)', '_1200', image_url)

        return modified_url + query_params  # Append query parameters if they exist

    def download_image(self, image_url: str, product_name: str, timestamp: str, 
                    image_folder: str, unique_id: str, retries: int = 3) -> str:
        """Synchronous image download method with enhanced error handling"""
        if not image_url or image_url == "N/A":
            return "N/A"

        # Clean filename
        image_filename = f"{unique_id}_{timestamp}.jpg"
        image_full_path = os.path.join(image_folder, image_filename)
        
        # Try both original and modified URLs
        urls_to_try = [
            self.modify_image_url(image_url),  # Try modified first (higher resolution)
            image_url  # Fallback to original URL
        ]
        
        for url_to_try in urls_to_try:
            for attempt in range(retries):
                try:
                    response = requests.get(url_to_try, timeout=30)
                    
                    # Check for 404 and skip to next URL if found
                    if response.status_code == 404:
                        print(f"URL not found (404): {url_to_try}")
                        break  # Break out of retry loop for this URL
                    
                    response.raise_for_status()
                    
                    # Verify it's actually an image
                    content_type = response.headers.get('content-type', '')
                    if not content_type.startswith('image/'):
                        print(f"URL returned non-image content type: {content_type}")
                        continue
                        
                    # Check if we got a valid image file (not too small)
                    if len(response.content) < 1024:  # Less than 1KB
                        print(f"Image too small, likely not valid: {len(response.content)} bytes")
                        continue
                        
                    with open(image_full_path, "wb") as f:
                        f.write(response.content)
                    
                    # Verify the file was written successfully
                    if os.path.exists(image_full_path) and os.path.getsize(image_full_path) > 0:
                        print(f"Successfully downloaded image for {product_name}")
                        return image_full_path
                    else:
                        print("File was not written successfully")
                        continue
                        
                except requests.RequestException as e:
                    print(f"Retry {attempt + 1}/{retries} - Error downloading {product_name}: {e}")
                    if attempt < retries - 1:
                        import time
                        time.sleep(2)  # Wait before retry
        
        print(f"Failed to download {product_name} after all attempts.")
        return "N/A"
    
    def extract_price_value(self, text: str) -> str:
        """Extract price from text"""
        if not text:
            return "N/A"
        
        price_patterns = [
            r'\$[\d,]+\.?\d*',
            r'\$\d{1,3}(?:,\d{3})*(?:\.\d{2})?',
        ]
        
        for pattern in price_patterns:
            price_match = re.search(pattern, text)
            if price_match:
                return price_match.group(0)
        
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
            r'(\d+/\d+)\s*ct',
            r'(\d+(?:\.\d+)?)\s*ct',
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
            r'(\d{1,2}K)\s*(?:Yellow|White|Rose)\s*Gold',
            r'(Yellow|White|Rose)\s*Gold\s*(\d{1,2}K)',
            r'(\d{1,2}K)\s*Gold',
            r'(Platinum|Sterling Silver|Silver)',
            r'(Yellow Gold|White Gold|Rose Gold)',
        ]
        
        for pattern in gold_patterns:
            gold_match = re.search(pattern, text, re.IGNORECASE)
            if gold_match:
                gold_parts = [part for part in gold_match.groups() if part]
                return ' '.join(gold_parts).title()
        
        return "N/A"