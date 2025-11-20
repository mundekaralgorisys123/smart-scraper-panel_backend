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


class ShaneCoScraper:
    """Scraper for Shane Co product pages with database and Excel functionality"""
    
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
            print("=================== Starting Shane Co Scraper ==================")
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
            image_folder = os.path.join(self.image_save_path, f"shaneco_{timestamp}")
            os.makedirs(image_folder, exist_ok=True)
            
            # Create Excel file
            excel_filename = f"shaneco_scraped_products_{timestamp}.xlsx"
            excel_path = os.path.join(self.excel_data_path, excel_filename)
            
            # Process products
            database_records = []
            successful_downloads = 0
            
            # Create Excel workbook
            wb = Workbook()
            sheet = wb.active
            sheet.title = "Shane Co Products"
            
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
                'website_type': 'shaneco',
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
        """Parse individual product HTML using your specific selectors"""
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
        """Extract individual product HTML blocks from Shane Co HTML"""
        if not html_content:
            return []
        
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Match your Playwright selectors exactly
        product_selectors = [
            'div.tile-container',  # Your main product container
            'li.product-category-carousel__grid_list_item-redesign',  # List item container
            'div.pos-relative',  # Position relative container
            '[auto-test="product-tile-test"]'  # Test attribute
        ]
        
        individual_products = []
        
        for selector in product_selectors:
            product_tiles = soup.select(selector)
            for tile in product_tiles:
                individual_products.append(str(tile))
            if individual_products:
                break
        
        print(f"Found {len(individual_products)} product tiles in Shane Co HTML")
        return individual_products
    
    def _extract_product_name(self, soup) -> str:
        """Extract product name using your exact selector"""
        name_element = soup.select_one('h3.text-body-menu.product-details__name-value')
        if name_element:
            product_name = self.clean_text(name_element.get_text())
            
            # Enhance product name with gold type if not already included
            gold_type = self._extract_gold_type_from_metal_container(soup)
            if gold_type and gold_type != "N/A" and gold_type.lower() not in product_name.lower():
                product_name = f"{product_name} - {gold_type}"
            
            return product_name
        return "N/A"
    
    def _extract_price(self, soup) -> str:
        """Extract price using your exact selector"""
        price_element = soup.select_one('div.product-details__price-center-stone-container h4.text-body-strong')
        if price_element:
            price_text = price_element.get_text(strip=True)
            return self.extract_price_value(price_text)
        return "N/A"
    
    def _extract_image(self, soup) -> str:
        """Extract image URL using your exact selector and data-url attribute"""
        image_element = soup.select_one('img.product-image')
        if image_element:
            image_url = image_element.get('data-url') or image_element.get('src')
            return self._normalize_image_url(image_url)
        return "N/A"
    
    def _extract_link(self, soup) -> str:
        """Extract product link"""
        link_element = soup.select_one('a.product-tile-container')
        if link_element and link_element.get('href'):
            href = link_element.get('href')
            return self._normalize_link_url(href)
        return "N/A"
    
    def _extract_diamond_weight(self, soup) -> str:
        """Extract diamond weight from product name"""
        product_name = self._extract_product_name(soup)
        return self.extract_diamond_weight_value(product_name)
    
    def _extract_gold_type(self, soup) -> str:
        """Extract gold type from metal type container"""
        gold_type = self._extract_gold_type_from_metal_container(soup)
        if gold_type != "N/A":
            return gold_type
        
        # Fallback to text-caption-small div
        material_element = soup.select_one('div.text-caption-small')
        if material_element:
            material_text = self.clean_text(material_element.get_text())
            gold_match = re.search(r'(\d+k\s+(?:Yellow|White|Rose)\s+Gold)', material_text, re.IGNORECASE)
            if gold_match:
                return gold_match.group(1)
            return material_text
        
        return "N/A"
    
    def _extract_gold_type_from_metal_container(self, soup) -> str:
        """Extract gold type specifically from the metal type container"""
        # Method 1: Get from the text in product-details__metal-type-container
        metal_container = soup.select_one('div.product-details__metal-type-container')
        if metal_container:
            # Get text from the text-caption-small div inside metal container
            text_element = metal_container.select_one('div.text-caption-small')
            if text_element:
                metal_text = self.clean_text(text_element.get_text())
                if metal_text:
                    return metal_text
            
            # Method 2: Get from title attribute of selected metal-color-option
            selected_metal = metal_container.select_one('div.metal-color-option.selected')
            if selected_metal and selected_metal.get('title'):
                return selected_metal.get('title')
        
        return "N/A"
    
    def _extract_badges(self, soup) -> list:
        """Extract badges and additional info using your exact selectors"""
        badges = []
        
        # 1. Extract tags like "Lab-Grown" - matching your Playwright code
        tag_elements = soup.select('span.badge span.text-caption-small')
        if tag_elements:
            for tag_element in tag_elements:
                tag_text = self.clean_text(tag_element.get_text())
                if tag_text:
                    badges.append(tag_text)
        
        # 2. Extract metal type from the dedicated container
        gold_type = self._extract_gold_type_from_metal_container(soup)
        if gold_type and gold_type != "N/A":
            badges.append(gold_type)
        
        # 3. Extract rating count
        rating_element = soup.select_one('div.pcat-ratings span.totalRattings')
        if rating_element:
            rating_text = self.clean_text(rating_element.get_text())
            if rating_text and "(" in rating_text:
                rating_count = rating_text.strip().strip("()")
                badges.append(f"{rating_count} ratings")
        
        return badges
    
    def _extract_promotions(self, soup) -> str:
        """Extract promotion text - Shane Co doesn't seem to have prominent promotions"""
        # Look for any promotional text in the product
        promo_selectors = [
            '.badge-fav-container',
            '[class*="promo"]',
            '[class*="sale"]',
            '.tag-text'
        ]
        
        for selector in promo_selectors:
            promo_elements = soup.select(selector)
            promo_texts = []
            for promo in promo_elements:
                promo_text = self.clean_text(promo.get_text())
                if promo_text and ("off" in promo_text.lower() or "sale" in promo_text.lower() or "promo" in promo_text.lower()):
                    promo_texts.append(promo_text)
            
            if promo_texts:
                return " | ".join(promo_texts)
        
        return "N/A"
    
    def _normalize_image_url(self, url: str) -> str:
        """Normalize image URL for Shane Co"""
        if not url or url == "N/A":
            return "N/A"
        if url.startswith('http'):
            return url
        elif url.startswith('//'):
            return f"https:{url}"
        elif url.startswith('/'):
            return f"https://www.shaneco.com{url}"
        return url
    
    def _normalize_link_url(self, url: str) -> str:
        """Normalize link URL for Shane Co"""
        if not url or url == "N/A":
            return "N/A"
        if url.startswith('http'):
            return url
        elif url.startswith('//'):
            return f"https:{url}"
        elif url.startswith('/'):
            return f"https://www.shaneco.com{url}"
        return url

    def modify_image_url(self, image_url: str) -> str:
        """Modify the image URL to get higher resolution images from Shane Co"""
        if not image_url or image_url == "N/A":
            return image_url

        # Shane Co specific image URL modifications
        modified_url = image_url
        
        # Replace scale parameters for higher quality
        modified_url = re.sub(r'scale=\.\d+', 'scale=1.0', modified_url)
        
        # Replace width and height parameters for larger images
        modified_url = re.sub(r'wid=\d+', 'wid=1200', modified_url)
        modified_url = re.sub(r'hei=\d+', 'hei=1200', modified_url)
        
        # Replace image format for better quality if needed
        if 'fmt=png-alpha' in modified_url:
            modified_url = modified_url.replace('fmt=png-alpha', 'fmt=jpeg')
        
        return modified_url

    def download_image(self, image_url: str, product_name: str, timestamp: str, 
                      image_folder: str, unique_id: str, retries: int = 3) -> str:
        """Synchronous image download method with enhanced error handling"""
        if not image_url or image_url == "N/A":
            return "N/A"

        # Clean filename
        image_filename = f"{unique_id}_{timestamp}.jpg"
        image_full_path = os.path.join(image_folder, image_filename)
        modified_url = self.modify_image_url(image_url)
        
        for attempt in range(retries):
            try:
                response = requests.get(modified_url, timeout=30)
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
        
        price_match = re.search(r'\$\d{1,3}(?:,\d{3})*(?:\.\d{2})?', text)
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
            r'(\d+k)\s*(?:Yellow|White|Rose)\s*Gold',
            r'(Yellow|White|Rose)\s*Gold\s*(\d+k)',
            r'(\d+k)\s*Gold',
            r'(Platinum|Sterling Silver|Silver)',
            r'(Yellow Gold|White Gold|Rose Gold)',
        ]
        
        for pattern in gold_patterns:
            gold_match = re.search(pattern, text, re.IGNORECASE)
            if gold_match:
                gold_parts = [part for part in gold_match.groups() if part]
                return ' '.join(gold_parts).title()
        
        return "N/A"