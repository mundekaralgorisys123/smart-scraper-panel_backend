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
from database.db_inseartin import insert_into_db, update_product_count

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

IMAGE_SAVE_PATH = os.getenv("IMAGE_SAVE_PATH")
EXCEL_DATA_PATH = os.getenv("EXCEL_DATA_PATH")


class KayOutletParser:
    """Parser for Kay Outlet product pages with database and Excel functionality"""
    
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
            print("=================== Starting Kay Outlet Parser ==================")
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
            image_folder = os.path.join(self.image_save_path, f"kayoutlet_{timestamp}")
            os.makedirs(image_folder, exist_ok=True)
            
            # Create Excel file
            excel_filename = f"kayoutlet_scraped_products_{timestamp}.xlsx"
            excel_path = os.path.join(self.excel_data_path, excel_filename)
            
            # Process products
            database_records = []
            successful_downloads = 0
            
            # Create Excel workbook
            wb = Workbook()
            sheet = wb.active
            sheet.title = "Kay Outlet Products"
            
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
                'website_type': 'kayoutlet',
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
        """Extract individual product HTML blocks from Kay Outlet HTML"""
        if not html_content:
            return []
        
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Multiple ways to find Kay Outlet products - similar structure to other Signet sites
        product_selectors = [
            'div.product-grid_tile',  # Main product container
            'div.product-item',       # Product item
            'app-product-grid-item-akron',  # Angular component
            'div[data-product-id]',   # Products with data attributes
            '.product-tile',          # Alternative product tile class
            '.prod-row-item'          # Kay Outlet specific
        ]
        
        individual_products = []
        
        for selector in product_selectors:
            product_tiles = soup.select(selector)
            for tile in product_tiles:
                individual_products.append(str(tile))
            if individual_products:
                break  # Stop if we found products with this selector
        
        print(f"Found {len(individual_products)} product tiles in Kay Outlet HTML")
        return individual_products
    
    def _extract_product_name(self, soup) -> str:
        """Extract product name from Kay Outlet product tile"""
        # Try multiple selectors for product name
        name_selectors = [
            'h2.name a',  # Product name in header
            '.product-tile-description a',  # Product description
            'a[itemprop="url"]',  # Item prop URL
            '.js-product-name-details a',  # JavaScript product name
            '.name a',  # Simple name selector
            'h3 a'  # Alternative header
        ]
        
        for selector in name_selectors:
            name_element = soup.select_one(selector)
            if name_element and name_element.get_text(strip=True):
                return self.clean_text(name_element.get_text())
        
        return "N/A"
    
    # def _extract_price(self, soup) -> str:
    #     """Extract price information from Kay Outlet product"""
    #     # Current price selectors
    #     price_selectors = [
    #         '.price .plp-align',  # Current price
    #         '.product-prices .price',  # Price container
    #         '.pj-price',  # Price wrapper
    #         '[data-di-id*="price"]',  # Data attribute
    #         '.current-price',  # Current price alternative
    #         '.sales-price',  # Sales price
    #         '.groupby-red-nowprice-font'  # Kay Outlet specific price class
    #     ]
        
    #     for selector in price_selectors:
    #         price_element = soup.select_one(selector)
    #         if price_element:
    #             price_text = price_element.get_text(strip=True)
    #             extracted_price = self.extract_price_value(price_text)
    #             if extracted_price != "N/A":
    #                 return extracted_price
        
    #     # Look for price in any text
    #     price_match = re.search(r'\$\d{1,3}(?:,\d{3})*(?:\.\d{2})?', soup.get_text())
    #     if price_match:
    #         return price_match.group(0)
        
    #     return "N/A"

    def _extract_price(self, soup) -> str:
        """Extract price, discount, and original price for Zales product"""
        
        html_text = soup.get_text(" ", strip=True)

        # --- 1) Extract sale price ---
        sale_price_match = re.search(r'\$\d{1,3}(?:,\d{3})*(?:\.\d{2})?', html_text)
        sale_price = sale_price_match.group(0) if sale_price_match else "N/A"

        # --- 2) Extract discount ---
        discount_match = re.search(r'(\d+% off)', html_text, re.IGNORECASE)
        discount = discount_match.group(0) if discount_match else "N/A"

        # --- 3) Calculate original price ---
        original_price = "N/A"
        if sale_price != "N/A" and discount != "N/A":
            try:
                discount_percent = int(re.search(r'(\d+)%', discount).group(1))
                sale_value = float(sale_price.replace("$", "").replace(",", ""))
                original_value = sale_value / (1 - (discount_percent / 100))
                original_price = f"${original_value:,.2f}"
            except:
                original_price = "N/A"

        return f"{sale_price} | {discount} | {original_price}"
    
    def _extract_image(self, soup) -> str:
        """Extract product image URL from Kay Outlet product"""
        # Image selectors
        img_selectors = [
            'img[itemprop="image"]',  # Schema image
            '.main-thumb img',  # Main thumbnail
            'app-product-primary-image img',  # Primary image component
            'img.plpimage',  # PLP image
            'img[src*="productimages"]',  # Product images
            '.product-image img',  # Product image
            'img[src*="kayoutlet.com"]'  # Kay Outlet specific images
        ]
        
        for selector in img_selectors:
            img_element = soup.select_one(selector)
            if img_element and img_element.get('src'):
                src = img_element.get('src')
                return self._normalize_image_url(src)
        
        return "N/A"
    
    def _extract_link(self, soup) -> str:
        """Extract product link from Kay Outlet product"""
        # Link selectors
        link_selectors = [
            'h2.name a',  # Name link
            '.main-thumb',  # Thumbnail link
            'a[itemprop="url"]',  # Schema URL
            '.product-tile-description a',  # Description link
            '.name a',  # Simple name link
            'a.product-link'  # Product link class
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
        """Extract badge information from Kay Outlet product"""
        badges = []
        
        # Badge selectors
        badge_selectors = [
            '.product-tag',  # Product tags
            '.secondary-badge .tag-container span',  # Secondary badges
            '.badge-container span',  # Badge container
            '.groupby-tablet-product-tags',  # Group badges
            '.badge',  # Simple badge
            '.promo-badge',  # Promotion badge
            'app-image-badge img'  # Kay Outlet badge images
        ]
        
        for selector in badge_selectors:
            badge_elements = soup.select(selector)
            for badge in badge_elements:
                # For images, get alt text
                if badge.name == 'img' and badge.get('alt'):
                    badge_text = self.clean_text(badge.get('alt'))
                else:
                    badge_text = self.clean_text(badge.get_text())
                
                if badge_text and badge_text not in badges and badge_text != "badge image":
                    badges.append(badge_text)
        
        return badges
    
    def _extract_promotions(self, soup) -> str:
        """Extract promotion text from Kay Outlet product"""
        # Promotion selectors
        promo_selectors = [
            '.tag-text',  # Discount tags
            '.amor-tags .tag-text',  # Amor tags
            '.discount-percentage',  # Discount percentage
            '[class*="promotion"]',  # Any promotion class
            '.sale-text',  # Sale text
            '.promo-text'  # Promotion text
        ]
        
        for selector in promo_selectors:
            promo_elements = soup.select(selector)
            promo_texts = []
            for promo in promo_elements:
                promo_text = self.clean_text(promo.get_text())
                if promo_text and ("off" in promo_text.lower() or "sale" in promo_text.lower() or "%" in promo_text):
                    promo_texts.append(promo_text)
            
            if promo_texts:
                return " | ".join(promo_texts)
        
        return "N/A"
    
    def _normalize_image_url(self, url: str) -> str:
        """Normalize image URL for Kay Outlet"""
        if not url:
            return "N/A"
        if url.startswith('http'):
            return url
        elif url.startswith('//'):
            return f"https:{url}"
        elif url.startswith('/'):
            return f"https://www.kayoutlet.com{url}"
        return url
    
    def _normalize_link_url(self, url: str) -> str:
        """Normalize link URL for Kay Outlet"""
        if not url:
            return "N/A"
        if url.startswith('http'):
            return url
        elif url.startswith('//'):
            return f"https:{url}"
        elif url.startswith('/'):
            return f"https://www.kayoutlet.com{url}"
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
                    time.sleep(2)  # Wait before retry
        
        logger.error(f"Failed to download {product_name} after {retries} attempts.")
        return "N/A"
    
    def extract_price_value(self, text: str) -> str:
        """Extract price from text"""
        if not text:
            return "N/A"
        
        # Look for price patterns
        price_patterns = [
            r'\$[\d,]+\.?\d*',  # Standard price format
            r'\$\d{1,3}(?:,\d{3})*(?:\.\d{2})?',  # Formatted price
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
    
    def extract_diamond_weight_value(self, text: str) -> str:
        """Extract diamond weight from text"""
        if not text:
            return "N/A"
        
        # Diamond weight patterns for Kay Outlet
        weight_patterns = [
            r'(\d+(?:\/\d+)?)\s*ct\s*tw',  # "1/3 ct tw" or "1/20 ct tw"
            r'(\d+(?:\.\d+)?)\s*ct\s*tw',  # "1.5 ct tw"
            r'(\d+(?:\.\d+)?)\s*ctw',  # "1.5ctw"
            r'(\d+(?:\.\d+)?)\s*carat',  # "1.5 carat"
            r'(\d+/\d+)\s*ct',  # "1/2 ct"
            r'(\d+-\d+/\d+)\s*ct',  # "1-1/2 ct"
            r'(\d+(?:\.\d+)?)\s*ct',  # "1.5 ct"
            r'(\d+(?:\.\d+)?)\s*carats'  # "1.5 carats"
        ]
        
        for pattern in weight_patterns:
            weight_match = re.search(pattern, text, re.IGNORECASE)
            if weight_match:
                weight = weight_match.group(1)
                # Standardize the format
                if 'tw' not in text.lower():
                    return f"{weight} ct tw"
                return f"{weight} ct"
        
        return "N/A"
    
    def extract_gold_type_value(self, text: str) -> str:
        """Extract gold type from text"""
        if not text:
            return "N/A"
        
        # Gold type patterns for Kay Outlet
        gold_patterns = [
            r'(\d{1,2}K)\s*(?:Yellow|White|Rose)\s*Gold',  # "14K Yellow Gold"
            r'(Yellow|White|Rose)\s*Gold\s*(\d{1,2}K)',  # "Yellow Gold 14K"
            r'(\d{1,2}K)\s*Gold',  # "14K Gold"
            r'(Platinum|Sterling Silver|Silver)',  # Other metals
            r'(Yellow Gold|White Gold|Rose Gold)',  # Gold colors
            r'(\d{1,2}K)\s*(?:YG|WG|RG)',  # "14K YG"
            r'(White|Yellow|Rose)\s*(\d{1,2}K)',  # "White 14K"
            r'in\s*(\d{1,2}K)\s*(?:White|Yellow|Rose)\s*Gold'  # "in 14K White Gold"
        ]
        
        for pattern in gold_patterns:
            gold_match = re.search(pattern, text, re.IGNORECASE)
            if gold_match:
                # Return the matched groups, filtering out None
                gold_parts = [part for part in gold_match.groups() if part]
                return ' '.join(gold_parts).title()
        
        return "N/A"