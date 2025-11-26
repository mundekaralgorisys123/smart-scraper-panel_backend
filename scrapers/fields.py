import asyncio
import base64
import os
import uuid
import logging
from datetime import datetime
from bs4 import BeautifulSoup
import re
from typing import Dict, Any, List
import httpx
from urllib.parse import urlparse
from openpyxl import Workbook
from database.db_inseartin import insert_into_db, update_product_count

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

IMAGE_SAVE_PATH = os.getenv("IMAGE_SAVE_PATH")
EXCEL_DATA_PATH = os.getenv("EXCEL_DATA_PATH")


class FieldsScraper:
    """Parser for Fields.ie product pages with database and Excel functionality"""
    
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
            print("=================== Starting Fields Scraper ==================")
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
            image_folder = os.path.join(self.image_save_path, f"fields_{timestamp}")
            os.makedirs(image_folder, exist_ok=True)
            
            # Create Excel file
            excel_filename = f"fields_scraped_products_{timestamp}.xlsx"
            excel_path = os.path.join(self.excel_data_path, excel_filename)
            
            # Process products
            database_records = []
            successful_downloads = 0
            
            # Create Excel workbook
            wb = Workbook()
            sheet = wb.active
            sheet.title = "Fields Products"
            
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
                    
                    # Skip if essential data is missing
                    if (parsed_data.get('product_name') == "N/A" or 
                        parsed_data.get('image_url') == "N/A"):
                        print(f"Skipping product due to missing data: Name: {parsed_data.get('product_name')}, Image: {parsed_data.get('image_url')}")
                        continue
                    
                    # Generate unique ID
                    unique_id = str(uuid.uuid4())
                    product_name = parsed_data.get('product_name', 'Unknown Product')[:495]
                    
                    # Download image - use async method
                    image_url = parsed_data.get('image_url')
                    image_path = asyncio.run(self.download_image_async(
                        image_url, product_name, timestamp, image_folder, unique_id
                    ))
                    
                    if image_path != "N/A":
                        successful_downloads += 1
                    
                    # Prepare additional info
                    badges = parsed_data.get('badges', [])
                    promotions = parsed_data.get('promotions', '')
                    additional_info_parts = []
                    
                    if badges and badges != ["N/A"]:
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
            if database_records:
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
                'website_type': 'fields',
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
        try:
            soup = BeautifulSoup(product_html, 'html.parser')
            
            product_name = self._extract_product_name(soup)
            price = self._extract_price(soup)
            image_url = self._extract_image(soup)
            link = self._extract_link(soup)
            
            # Skip if essential data is missing
            if product_name == "N/A" or image_url == "N/A":
                return {
                    'product_name': product_name,
                    'price': price,
                    'image_url': image_url,
                    'link': link,
                    'diamond_weight': "N/A",
                    'gold_type': "N/A",
                    'badges': ["N/A"],
                    'promotions': "N/A"
                }
            
            return {
                'product_name': product_name,
                'price': price,
                'image_url': image_url,
                'link': link,
                'diamond_weight': self._extract_diamond_weight(product_name),
                'gold_type': self._extract_gold_type(product_name),
                'badges': self._extract_badges(soup),
                'promotions': self._extract_promotions(soup)
            }
        except Exception as e:
            logger.error(f"Error parsing product: {e}")
            return {
                'product_name': "N/A",
                'price': "N/A",
                'image_url': "N/A",
                'link': "N/A",
                'diamond_weight': "N/A",
                'gold_type': "N/A",
                'badges': ["N/A"],
                'promotions': "N/A"
            }
    
    def extract_individual_products_from_html(self, html_content: str) -> List[str]:
        """Extract individual product HTML blocks from Fields HTML"""
        if not html_content:
            return []
        
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Fields specific product selectors (Salesforce Commerce Cloud)
        product_selectors = [
            'div.product-tile',  # Main product container
            'div.product',       # Product wrapper
            '.tile-container .product',  # Product in tile container
            '[data-pid]'         # Products with product ID
        ]
        
        individual_products = []
        
        for selector in product_selectors:
            product_tiles = soup.select(selector)
            for tile in product_tiles:
                individual_products.append(str(tile))
            if individual_products:
                break  # Stop if we found products with this selector
        
        print(f"Found {len(individual_products)} product tiles in Fields HTML")
        return individual_products
    
    def _extract_product_name(self, soup) -> str:
        """Extract product name from Fields product tile"""
        # Try multiple selectors for product name
        name_selectors = [
            '.pdp-link .link',  # Product name in pdp link
            '.tile-body .link',  # Link in tile body
            'a.link',  # Any link with class link
            '[data-gtm] a'  # Link with GTM data
        ]
        
        for selector in name_selectors:
            name_element = soup.select_one(selector)
            if name_element and name_element.get_text(strip=True):
                return self.clean_text(name_element.get_text())
        
        return "N/A"
    
    def _extract_price(self, soup) -> str:
        """Extract price information from Fields product"""
        try:
            # Fields has different price structure - original and sale prices
            original_price_element = soup.select_one('del .value')
            sale_price_element = soup.select_one('.sales .value')
            
            original_price = original_price_element.get_text(strip=True) if original_price_element else ""
            sale_price = sale_price_element.get_text(strip=True) if sale_price_element else ""
            
            if sale_price and original_price:
                return f"{sale_price} | Was {original_price}"
            elif sale_price:
                return sale_price
            elif original_price:
                return original_price
            else:
                # Try to find any price value
                price_element = soup.select_one('.price-content .value')
                if price_element:
                    return price_element.get_text(strip=True)
                return "N/A"
                
        except Exception as e:
            logger.warning(f"Error extracting price: {e}")
            return "N/A"
    
    def _extract_image(self, soup) -> str:
        """Extract product image URL from Fields product"""
        try:
            # Fields uses main image and hover image
            img_element = soup.select_one('.tile-image')
            if img_element:
                image_url = img_element.get('src')
                if image_url:
                    return self._normalize_image_url(image_url)
            
            return "N/A"
        except Exception as e:
            logger.warning(f"Error extracting image: {e}")
            return "N/A"
    
    def _extract_link(self, soup) -> str:
        """Extract product link from Fields product"""
        # Link selectors for Fields
        link_selectors = [
            '.pdp-link a.link',  # PDP link
            '.image-container a.figure',  # Image link
            'a.figure',  # Figure link
            'a[href*=".html"]'  # Any HTML link
        ]
        
        for selector in link_selectors:
            link_element = soup.select_one(selector)
            if link_element and link_element.get('href'):
                href = link_element.get('href')
                return self._normalize_link_url(href)
        
        return "N/A"
    
    def _extract_diamond_weight(self, product_name: str) -> str:
        """Extract diamond weight from product name"""
        if not product_name or product_name == "N/A":
            return "N/A"
        
        # Fields uses different diamond weight formats like "1.00ct", "0.40ct", etc.
        diamond_patterns = [
            r"(\d+\.\d+)\s*ct",  # 1.00ct, 0.40ct
            r"(\d+)\s*ct",       # 1ct
            r"(\d+\.\d+)\s*carat",  # 1.00 carat
            r"(\d+)\s*carat"     # 1 carat
        ]
        
        for pattern in diamond_patterns:
            diamond_match = re.search(pattern, product_name, re.IGNORECASE)
            if diamond_match:
                weight = diamond_match.group(1)
                return f"{weight} CT"
        
        return "N/A"
    
    def _extract_gold_type(self, product_name: str) -> str:
        """Extract gold type from product name"""
        if not product_name or product_name == "N/A":
            return "N/A"
        
        # Try multiple gold type patterns for Fields
        gold_patterns = [
            r"\b\d{1,2}ct\s+(?:yellow|white|rose)\s+gold\b",
            r"\b(?:9ct|9ct|14k|18k|24k)\s+(?:yellow|white|rose)\s+gold\b",
            r"\b(?:yellow|white|rose)\s+gold\b",
            r"\bsterling\s+silver\b",
            r"\bplatinum\b"
        ]
        
        for pattern in gold_patterns:
            gold_type_match = re.search(pattern, product_name, re.IGNORECASE)
            if gold_type_match:
                return gold_type_match.group().upper()
        
        return "N/A"
    
    def _extract_badges(self, soup) -> list:
        """Extract badge information from Fields product"""
        badges = []
        
        try:
            # Extract sale badges and other badges
            badge_elements = soup.select('.tile-badges .lozenges')
            for badge_element in badge_elements:
                badge_text = self.clean_text(badge_element.get_text())
                if badge_text:
                    badges.append(badge_text)
        except Exception as e:
            logger.warning(f"Error extracting badges: {e}")
        
        return badges if badges else ["N/A"]
    
    def _extract_promotions(self, soup) -> str:
        """Extract promotion text from Fields product"""
        # For Fields, the discount percentage serves as promotion info
        badge_element = soup.select_one('.tile-badges .lozenges')
        if badge_element:
            badge_text = badge_element.get_text(strip=True)
            return badge_text if badge_text else "N/A"
        
        return "N/A"
    
    def _normalize_image_url(self, url: str) -> str:
        """Normalize image URL for Fields"""
        if not url or url == "N/A":
            return "N/A"
        if url.startswith('http'):
            return url
        elif url.startswith('//'):
            return f"https:{url}"
        elif url.startswith('/'):
            return f"https://www.fields.ie{url}"
        return url
    
    def _normalize_link_url(self, url: str) -> str:
        """Normalize link URL for Fields"""
        if not url or url == "N/A":
            return "N/A"
        if url.startswith('http'):
            return url
        elif url.startswith('//'):
            return f"https:{url}"
        elif url.startswith('/'):
            return f"https://www.fields.ie{url}"
        return url

    def modify_image_url(self, image_url: str) -> str:
        """Modify the image URL to request higher resolution."""
        if not image_url or image_url == "N/A":
            return image_url

        # Fields uses sw and sh parameters for image dimensions
        # Increase dimensions for higher resolution
        if "sw=" in image_url and "sh=" in image_url:
            image_url = re.sub(r'sw=\d+', 'sw=2000', image_url)
            image_url = re.sub(r'sh=\d+', 'sh=2000', image_url)
        
        return image_url

    async def download_image_async(self, image_url, product_name, timestamp, image_folder, unique_id, retries=3):
        """Async image download with high-resolution preference"""
        if not image_url or image_url == "N/A":
            return "N/A"

        image_filename = f"{unique_id}_{timestamp}.jpg"
        image_full_path = os.path.join(image_folder, image_filename)

        high_res_url = self.modify_image_url(image_url)

        async with httpx.AsyncClient(timeout=30.0) as client:
            # Try high-resolution first
            for attempt in range(retries):
                try:
                    response = await client.get(high_res_url)
                    response.raise_for_status()
                    
                    # Verify it's actually an image
                    content_type = response.headers.get('content-type', '')
                    if not content_type.startswith('image/'):
                        logger.warning(f"URL {high_res_url} returned non-image content type: {content_type}")
                        continue
                        
                    with open(image_full_path, "wb") as f:
                        f.write(response.content)
                    
                    logger.info(f"Successfully downloaded high-res image for {product_name}")
                    return image_full_path
                    
                except httpx.RequestError as e:
                    logger.warning(f"Retry {attempt + 1}/{retries} - High-res failed for {product_name}: {e}")
                    if attempt < retries - 1:
                        await asyncio.sleep(2)  # Wait before retry
            
            # Fallback to original image
            try:
                response = await client.get(image_url)
                response.raise_for_status()
                
                # Verify it's actually an image
                content_type = response.headers.get('content-type', '')
                if not content_type.startswith('image/'):
                    logger.warning(f"Original URL {image_url} returned non-image content type: {content_type}")
                    return "N/A"
                    
                with open(image_full_path, "wb") as f:
                    f.write(response.content)
                
                logger.info(f"Successfully downloaded original image for {product_name}")
                return image_full_path
                
            except httpx.RequestError as e:
                logger.error(f"Fallback failed for {product_name}: {e}")
                return "N/A"
    
    def extract_price_value(self, text: str) -> str:
        """Extract price from text"""
        if not text:
            return "N/A"
        
        # Look for price patterns (Euro format)
        price_patterns = [
            r'€\s*[\d,]+\.?\d*',  # Euro price format
            r'€\s*\d{1,3}(?:,\d{3})*(?:\.\d{2})?',  # Formatted Euro price
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
        # Remove extra whitespace and normalize
        text = ' '.join(text.split()).strip()
        # Remove multiple spaces
        text = re.sub(r'\s+', ' ', text)
        return text