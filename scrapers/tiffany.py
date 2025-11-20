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


class TiffanyScraper:
    """Scraper for Tiffany & Co. product pages with database and Excel functionality"""
    
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
            print("=================== Starting Tiffany Scraper ==================")
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
            image_folder = os.path.join(self.image_save_path, f"tiffany_{timestamp}")
            os.makedirs(image_folder, exist_ok=True)
            
            # Create Excel file
            excel_filename = f"tiffany_scraped_products_{timestamp}.xlsx"
            excel_path = os.path.join(self.excel_data_path, excel_filename)
            
            # Process products
            database_records = []
            successful_downloads = 0
            
            # Create Excel workbook
            wb = Workbook()
            sheet = wb.active
            sheet.title = "Tiffany Products"
            
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
                'website_type': 'tiffany',
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
        """Parse individual product HTML using Tiffany's specific structure"""
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
        """Extract individual product HTML blocks from Tiffany HTML"""
        if not html_content:
            return []
        
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Tiffany specific product selectors
        product_selectors = [
            'li.ais-InfiniteHits-item',  # Main product list item
            'div.product-tile',  # Product tile container
            'div.product',  # Product container
            '.layout_1x1',  # Layout class from your Playwright code
            '[data-pid]'  # Products with product ID
        ]
        
        individual_products = []
        
        for selector in product_selectors:
            product_tiles = soup.select(selector)
            for tile in product_tiles:
                individual_products.append(str(tile))
            if individual_products:
                break
        
        print(f"Found {len(individual_products)} product tiles in Tiffany HTML")
        return individual_products
    
    def _extract_product_name(self, soup) -> str:
        """Extract product name from Tiffany product tile"""
        # Try multiple selectors for Tiffany structure
        name_selectors = [
            'h2.pdp-link',  # Main product link header
            '.pdp-link-collection',  # Collection name
            '.pdp-link-name',  # Product name
            'span.pdp-link-collection',  # Collection span
            'span.pdp-link-name',  # Product name span
        ]
        
        collection_parts = []
        product_parts = []
        
        # Extract collection name
        collection_element = soup.select_one('span.pdp-link-collection')
        if collection_element:
            collection_parts.append(self.clean_text(collection_element.get_text()))
        
        # Extract product name
        product_element = soup.select_one('span.pdp-link-name')
        if product_element:
            product_parts.append(self.clean_text(product_element.get_text()))
        
        # Combine collection and product name
        if collection_parts and product_parts:
            return f"{' '.join(collection_parts)} {' '.join(product_parts)}".strip()
        elif product_parts:
            return ' '.join(product_parts)
        elif collection_parts:
            return ' '.join(collection_parts)
        
        # Fallback to h2 text
        h2_element = soup.select_one('h2.pdp-link')
        if h2_element:
            return self.clean_text(h2_element.get_text())
        
        return "N/A"
    
    def _extract_price(self, soup) -> str:
        """Extract price from Tiffany product"""
        price_selectors = [
            'span.sales .value',  # Sales price value
            '.price .sales .value',  # Price container
            '.price span.value',  # Price value
            '[class*="price"] span',  # Any price related span
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
        """Extract image URL from Tiffany product with multiple fallbacks"""
        # Try multiple image selectors and attributes
        img_selectors = [
            'img.tile-image',  # Main tile image
            'picture img',  # Picture element images
            '.image-container img',  # Image container
            'img[src*="media.tiffany.com"]',  # Tiffany media images
        ]
        
        for selector in img_selectors:
            img_elements = soup.select(selector)
            for img_element in img_elements:
                if img_element:
                    # Try multiple attribute sources in priority order
                    attributes_to_try = ['src', 'data-src', 'data-srcset', 'srcset']
                    
                    for attr in attributes_to_try:
                        image_url = img_element.get(attr)
                        if image_url:
                            normalized_url = self._process_image_url(image_url)
                            if normalized_url:
                                return normalized_url
        
        return "N/A"
    
    def _process_image_url(self, image_url: str) -> str:
        """Process image URL with multiple fallbacks and normalization"""
        if not image_url or image_url == "N/A":
            return "N/A"
        
        # Handle srcset (comma-separated URLs with descriptors)
        if ',' in image_url and ('w' in image_url or 'x' in image_url):
            urls = [url.strip().split()[0] for url in image_url.split(',') if url.strip()]
            if urls:
                image_url = urls[0]  # Take the first URL from srcset
        
        # Normalize the URL
        return self._normalize_image_url(image_url)
    
    def _extract_link(self, soup) -> str:
        """Extract product link from Tiffany product"""
        link_selectors = [
            'a.link[href*="tiffany.com"]',  # Product link
            'h2.pdp-link a',  # Header link
            '.pdp-link a',  # Pdp link
            'a[href*="/jewelry/"]',  # Jewelry links
            'a[data-url]',  # Links with data-url
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
        """Extract gold type from product name and data attributes"""
        product_name = self._extract_product_name(soup)
        
        # First try to extract from product name
        gold_type = self.extract_gold_type_value(product_name)
        if gold_type != "N/A":
            return gold_type
        
        # Try to extract from data attributes in the GTM data
        gtm_data = soup.select_one('div.gtm-selectitem-data')
        if gtm_data and gtm_data.get('data-gtm'):
            try:
                import json
                gtm_json = gtm_data.get('data-gtm')
                gtm_data = json.loads(gtm_json)
                
                # Check for material in GTM data
                if gtm_data.get('item_material'):
                    return gtm_data.get('item_material')
                if gtm_data.get('item_color'):
                    color = gtm_data.get('item_color')
                    if 'PLAT' in color:
                        return 'Platinum'
                    elif 'GOLD' in color.upper():
                        return color
            except:
                pass
        
        return "N/A"
    
    def _extract_badges(self, soup) -> list:
        """Extract badges and tags from Tiffany product"""
        badges = []
        
        # Extract from tile buttons (matching your Playwright code)
        badge_selectors = [
            'div.tile-buttons span',  # Tile buttons spans
            '.tile-badge',  # Tile badges
            '[class*="badge"]',  # Any badge class
            '.new-tag',  # New tags
        ]
        
        for selector in badge_selectors:
            badge_elements = soup.select(selector)
            for badge in badge_elements:
                badge_text = self.clean_text(badge.get_text())
                if badge_text and badge_text not in badges:
                    badges.append(badge_text)
        
        return badges
    
    def _extract_promotions(self, soup) -> str:
        """Extract promotion text from Tiffany product"""
        # Look for promotional elements
        promo_selectors = [
            '.promo-badge',
            '.sale-tag',
            '[class*="promo"]',
            '[class*="sale"]',
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
    
    def _normalize_image_url(self, url: str) -> str:
        """Normalize image URL for Tiffany"""
        if not url or url == "N/A":
            return "N/A"
        if url.startswith('http'):
            return url
        elif url.startswith('//'):
            return f"https:{url}"
        elif url.startswith('/'):
            return f"https://www.tiffany.com{url}"
        return url
    
    def _normalize_link_url(self, url: str) -> str:
        """Normalize link URL for Tiffany"""
        if not url or url == "N/A":
            return "N/A"
        if url.startswith('http'):
            return url
        elif url.startswith('//'):
            return f"https:{url}"
        elif url.startswith('/'):
            return f"https://www.tiffany.com{url}"
        return url

    def modify_image_url(self, image_url: str) -> str:
        """Modify the image URL to get higher resolution images from Tiffany"""
        if not image_url or image_url == "N/A":
            return image_url

        # Tiffany specific image URL modifications
        modified_url = image_url
        
        # Replace dimensions for higher quality - Tiffany uses hei/wid parameters
        modified_url = re.sub(r'hei=\d+', 'hei=2000', modified_url)
        modified_url = re.sub(r'wid=\d+', 'wid=2000', modified_url)
        
        # Ensure webp format for better quality
        if 'fmt=' in modified_url:
            modified_url = re.sub(r'fmt=[^&]+', 'fmt=webp', modified_url)
        else:
            modified_url += '&fmt=webp' if '?' in modified_url else '?fmt=webp'
        
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
            r'(\d+k)\s*(?:White|Yellow|Rose)?\s*Gold',
            r'(White|Yellow|Rose)\s*Gold\s*(\d+k)',
            r'(\d+k)\s*Gold',
            r'(Platinum|Sterling Silver|Silver)',
            r'(White Gold|Yellow Gold|Rose Gold)',
        ]
        
        for pattern in gold_patterns:
            gold_match = re.search(pattern, text, re.IGNORECASE)
            if gold_match:
                gold_parts = [part for part in gold_match.groups() if part]
                return ' '.join(gold_parts).title()
        
        return "N/A"