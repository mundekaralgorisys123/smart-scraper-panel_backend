import base64
import os
import uuid
from datetime import datetime
from bs4 import BeautifulSoup
import re
from typing import Dict, Any, List
import httpx
import requests
from urllib.parse import urlparse
from openpyxl import Workbook
from database.db_inseartin import insert_into_db, update_product_count

class JaredParser:
    """Parser for Jared product pages with database and Excel functionality"""
    
    def __init__(self, excel_data_path='static/ExcelData', image_save_path='static/Images'):
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
            print("=================== Starting Jared Parser ==================")
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
            image_folder = os.path.join(self.image_save_path, f"jared_{timestamp}")
            os.makedirs(image_folder, exist_ok=True)
            
            # Create Excel file
            excel_filename = f"jared_scraped_products_{timestamp}.xlsx"
            excel_path = os.path.join(self.excel_data_path, excel_filename)
            
            # Process products
            database_records = []
            successful_downloads = 0
            
            # Create Excel workbook
            wb = Workbook()
            sheet = wb.active
            sheet.title = "Jared Products"
            
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
                    
                    # Download image
                    image_url = parsed_data.get('image_url')
                    image_path = self.download_image_sync(
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
                'website_type': 'jared',
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
        """Extract individual product HTML blocks from Jared HTML"""
        if not html_content:
            return []
        
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Multiple ways to find Jared products
        product_selectors = [
            'div.product-grid_tile',  # Main product container
            'div.product-item',       # Product item
            'app-product-grid-item-akron',  # Angular component
            'div[data-product-id]'    # Products with data attributes
        ]
        
        individual_products = []
        
        for selector in product_selectors:
            product_tiles = soup.select(selector)
            for tile in product_tiles:
                individual_products.append(str(tile))
            if individual_products:
                break  # Stop if we found products with this selector
        
        print(f"Found {len(individual_products)} product tiles in Jared HTML")
        return individual_products
    
    def _extract_product_name(self, soup) -> str:
        """Extract product name from Jared product tile"""
        # Try multiple selectors for product name
        name_selectors = [
            'h2.name a',  # Product name in header
            '.product-tile-description a',  # Product description
            'a[itemprop="url"]',  # Item prop URL
            '.js-product-name-details a'  # JavaScript product name
        ]
        
        for selector in name_selectors:
            name_element = soup.select_one(selector)
            if name_element and name_element.get_text(strip=True):
                return self.clean_text(name_element.get_text())
        
        return "N/A"
    
    def _extract_price(self, soup) -> str:
        """Extract price information from Jared product"""
        # Current price selectors
        price_selectors = [
            '.price .plp-align',  # Current price
            '.product-prices .price',  # Price container
            '.pj-price',  # Price wrapper
            '[data-di-id*="price"]'  # Data attribute
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
        """Extract product image URL from Jared product"""
        # Image selectors
        img_selectors = [
            'img[itemprop="image"]',  # Schema image
            '.main-thumb img',  # Main thumbnail
            'app-product-primary-image img',  # Primary image component
            'img.plpimage',  # PLP image
            'img[src*="productimages"]'  # Product images
        ]
        
        for selector in img_selectors:
            img_element = soup.select_one(selector)
            if img_element and img_element.get('src'):
                src = img_element.get('src')
                return self._normalize_image_url(src)
        
        return "N/A"
    
    def _extract_link(self, soup) -> str:
        """Extract product link from Jared product"""
        # Link selectors
        link_selectors = [
            'h2.name a',  # Name link
            '.main-thumb',  # Thumbnail link
            'a[itemprop="url"]',  # Schema URL
            '.product-tile-description a'  # Description link
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
        """Extract badge information from Jared product"""
        badges = []
        
        # Badge selectors
        badge_selectors = [
            '.product-tag',  # Product tags
            '.secondary-badge .tag-container span',  # Secondary badges
            '.badge-container span',  # Badge container
            '.groupby-tablet-product-tags'  # Group badges
        ]
        
        for selector in badge_selectors:
            badge_elements = soup.select(selector)
            for badge in badge_elements:
                badge_text = self.clean_text(badge.get_text())
                if badge_text and badge_text not in badges:
                    badges.append(badge_text)
        
        return badges
    
    def _extract_promotions(self, soup) -> str:
        """Extract promotion text from Jared product"""
        # Promotion selectors
        promo_selectors = [
            '.tag-text',  # Discount tags
            '.amor-tags .tag-text',  # Amor tags
            '.discount-percentage',  # Discount percentage
            '[class*="promotion"]'  # Any promotion class
        ]
        
        for selector in promo_selectors:
            promo_elements = soup.select(selector)
            promo_texts = []
            for promo in promo_elements:
                promo_text = self.clean_text(promo.get_text())
                if promo_text and "off" in promo_text.lower():
                    promo_texts.append(promo_text)
            
            if promo_texts:
                return " | ".join(promo_texts)
        
        return "N/A"
    
    def _normalize_image_url(self, url: str) -> str:
        """Normalize image URL for Jared"""
        if not url:
            return "N/A"
        if url.startswith('http'):
            return url
        elif url.startswith('//'):
            return f"https:{url}"
        elif url.startswith('/'):
            return f"https://www.jared.com{url}"
        return url
    
    def _normalize_link_url(self, url: str) -> str:
        """Normalize link URL for Jared"""
        if not url:
            return "N/A"
        if url.startswith('http'):
            return url
        elif url.startswith('//'):
            return f"https:{url}"
        elif url.startswith('/'):
            return f"https://www.jared.com{url}"
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

    def download_image_sync(self, image_url: str, product_name: str, timestamp: str, image_folder: str, unique_id: str) -> str:
        """Download image synchronously"""
        try:
            if not image_url or image_url == "N/A":
                return "N/A"
            
            # Clean product name for filename
            clean_name = "".join(c for c in product_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
            clean_name = clean_name[:50]
            
            # Get file extension
            parsed_url = urlparse(image_url)
            file_ext = os.path.splitext(parsed_url.path)[1]
            if not file_ext:
                file_ext = '.jpg'
            
            # Generate filename
            filename = f"{unique_id}_{clean_name}_{timestamp}{file_ext}"
            filepath = os.path.join(image_folder, filename)

            # Modify image URL for higher resolution
            modified_url = self.modify_image_url(image_url)
            
            # Download with retries
            retries = 3
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            for attempt in range(retries):
                try:
                    response = requests.get(modified_url, timeout=30, headers=headers)
                    if response.status_code == 200:
                        with open(filepath, 'wb') as f:
                            f.write(response.content)
                        return filepath
                    else:
                        print(f"Attempt {attempt + 1}: Failed to download image - Status {response.status_code}")
                except Exception as e:
                    print(f"Attempt {attempt + 1}: Error downloading {product_name}: {e}")
            
            print(f"Failed to download {product_name} after {retries} attempts.")
            return "N/A"
                
        except Exception as e:
            print(f"Error downloading image {image_url}: {e}")
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
    
    def extract_diamond_weight_value(self, text: str) -> str:
        """Extract diamond weight from text"""
        if not text:
            return "N/A"
        
        # Diamond weight patterns for Jared
        weight_patterns = [
            r'(\d+(?:\.\d+)?)\s*ct\s*tw',  # "1.5 ct tw"
            r'(\d+(?:\.\d+)?)\s*ctw',  # "1.5ctw"
            r'(\d+(?:\.\d+)?)\s*carat',  # "1.5 carat"
            r'(\d+/\d+)\s*ct',  # "1/2 ct"
            r'(\d+-\d+/\d+)\s*ct'  # "1-1/2 ct"
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
        
        # Gold type patterns for Jared
        gold_patterns = [
            r'(\d{1,2}K)\s*(?:Yellow|White|Rose)\s*Gold',  # "14K Yellow Gold"
            r'(Yellow|White|Rose)\s*Gold\s*(\d{1,2}K)',  # "Yellow Gold 14K"
            r'(\d{1,2}K)\s*Gold',  # "14K Gold"
            r'(Platinum|Sterling Silver|Silver)',  # Other metals
            r'(Yellow Gold|White Gold|Rose Gold)'  # Gold colors
        ]
        
        for pattern in gold_patterns:
            gold_match = re.search(pattern, text, re.IGNORECASE)
            if gold_match:
                # Return the matched groups, filtering out None
                gold_parts = [part for part in gold_match.groups() if part]
                return ' '.join(gold_parts).title()
        
        return "N/A"