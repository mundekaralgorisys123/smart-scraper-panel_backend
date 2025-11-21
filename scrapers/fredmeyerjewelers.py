import asyncio
import base64
import os
import time
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


class FredMeyerJewelersParser:
    """Parser for Fred Meyer Jewelers product pages with database and Excel functionality"""
    
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
            print("=================== Starting Fred Meyer Jewelers Parser ==================")
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
            image_folder = os.path.join(self.image_save_path, f"fredmeyer_{timestamp}")
            os.makedirs(image_folder, exist_ok=True)
            
            # Create Excel file
            excel_filename = f"fredmeyer_scraped_products_{timestamp}.xlsx"
            excel_path = os.path.join(self.excel_data_path, excel_filename)
            
            # Process products
            database_records = []
            successful_downloads = 0
            
            # Create Excel workbook
            wb = Workbook()
            sheet = wb.active
            sheet.title = "Fred Meyer Products"
            
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
                    print(f"Parsing product {i+1}/{len(individual_products)}")
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
                'website_type': 'fredmeyer',
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
        """Extract individual product HTML blocks from Fred Meyer Jewelers HTML"""
        if not html_content:
            return []
        
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Multiple ways to find Fred Meyer Jewelers products
        product_selectors = [
            'article.x-result',  # Main product container
            '.x-base-grid__result',  # Grid result item
            '[data-wysiwyg="result"]',  # Products with wysiwyg data
            '[data-test="search-grid-result"]',  # Search grid results
            '.x-base-grid__item'  # Grid items
        ]
        
        individual_products = []
        
        for selector in product_selectors:
            product_tiles = soup.select(selector)
            for tile in product_tiles:
                individual_products.append(str(tile))
            if individual_products:
                break  # Stop if we found products with this selector
        
        print(f"Found {len(individual_products)} product tiles in Fred Meyer Jewelers HTML")
        return individual_products
    
    def _extract_product_name(self, soup) -> str:
        """Extract product name from Fred Meyer Jewelers product tile"""
        # Try multiple selectors for product name
        name_selectors = [
            'h2[data-test="result-title"]',  # Result title
            '.x-text1-lg',  # Text class
            '[data-wysiwyg-title]',  # Wysiwyg title attribute
            '.x-result__description h2',  # Result description header
            'h2.x-line-clamp-2'  # Clamped title
        ]
        
        for selector in name_selectors:
            name_element = soup.select_one(selector)
            if name_element and name_element.get_text(strip=True):
                return self.clean_text(name_element.get_text())
        
        return "N/A"
    
    def _extract_price(self, soup) -> str:
        """Extract price information from Fred Meyer Jewelers product"""
        # Current price selectors
        price_selectors = [
            '[data-test="result-current-price"]',  # Current price
            '.x-result-current-price',  # Current price class
            '.x-currency',  # Currency class
            '.x-font-main.x-text-[15px]'  # Price text class
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
        """Extract product image URL from Fred Meyer Jewelers product"""
        # Image selectors
        img_selectors = [
            'img[data-test="result-picture-image"]',  # Result picture image
            '.x-result-picture-image',  # Result picture class
            'img.x-picture-image',  # Picture image class
            '[data-wysiwyg-image-url]',  # Wysiwyg image URL attribute
            '.x-result__picture img'  # Result picture img
        ]
        
        for selector in img_selectors:
            img_element = soup.select_one(selector)
            if img_element and img_element.get('src'):
                src = img_element.get('src')
                return self._normalize_image_url(src)
            
            # Check for data attribute
            if img_element and img_element.get('data-wysiwyg-image-url'):
                src = img_element.get('data-wysiwyg-image-url')
                return self._normalize_image_url(src)
        
        return "N/A"
    
    def _extract_link(self, soup) -> str:
        """Extract product link from Fred Meyer Jewelers product"""
        # Link selectors
        link_selectors = [
            'a[data-test="result-link"]',  # Result link
            '.x-result-link',  # Result link class
            'a.x-result__picture',  # Picture link
            'a.x-result__description'  # Description link
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
        """Extract badge information from Fred Meyer Jewelers product"""
        badges = []
        
        # Badge selectors - Fred Meyer Jewelers uses different badge system
        badge_selectors = [
            '.x-badge',  # Badge class
            '.x-badge-circle',  # Circle badge
            '[data-test*="badge"]',  # Test attributes with badge
            '.x-text2-lg'  # Text that might contain badge info
        ]
        
        for selector in badge_selectors:
            badge_elements = soup.select(selector)
            for badge in badge_elements:
                badge_text = self.clean_text(badge.get_text())
                if badge_text and badge_text not in badges:
                    # Filter out common non-badge text
                    if not any(excluded in badge_text.lower() for excluded in ['in stock', 'items:', 'results']):
                        badges.append(badge_text)
        
        return badges
    
    def _extract_promotions(self, soup) -> str:
        """Extract promotion text from Fred Meyer Jewelers product"""
        # Promotion selectors
        promo_selectors = [
            '[data-test="result-previous-price"]',  # Previous price (indicates sale)
            '.x-result-previous-price',  # Previous price class
            '.x-line-through'  # Strikethrough text (sale indication)
        ]
        
        promo_texts = []
        
        # Check for sale indicators
        for selector in promo_selectors:
            promo_elements = soup.select(selector)
            for promo in promo_elements:
                promo_text = self.clean_text(promo.get_text())
                if promo_text and promo_text.startswith('$'):
                    promo_texts.append(f"Was {promo_text}")
        
        # Check for any discount indicators in the text
        if "on-sale" in soup.get('class', []):
            promo_texts.append("On Sale")
        
        if promo_texts:
            return " | ".join(promo_texts)
        
        return "N/A"
    
    def _normalize_image_url(self, url: str) -> str:
        """Normalize image URL for Fred Meyer Jewelers"""
        if not url:
            return "N/A"
        if url.startswith('http'):
            return url
        elif url.startswith('//'):
            return f"https:{url}"
        elif url.startswith('/'):
            return f"https://www.fredmeyerjewelers.com{url}"
        return url
    
    def _normalize_link_url(self, url: str) -> str:
        """Normalize link URL for Fred Meyer Jewelers"""
        if not url:
            return "N/A"
        if url.startswith('http'):
            return url
        elif url.startswith('//'):
            return f"https:{url}"
        elif url.startswith('/'):
            return f"https://www.fredmeyerjewelers.com{url}"
        return url

   

    def download_image(image_url, product_name, timestamp, image_folder, unique_id, retries=5, timeout=30):
        if not image_url or image_url == "N/A":
            return "N/A"

        image_filename = f"{unique_id}_{timestamp}.jpg"
        image_full_path = os.path.join(image_folder, image_filename)
        print(f"Saving image to: {product_name}")
        print("imageurl:", image_url)
        print(f"Downloading image: {image_url} to {image_full_path}")

        for attempt in range(1, retries + 1):
            try:
                headers = {"User-Agent": "Mozilla/5.0"}
                response = requests.get(image_url, headers=headers, stream=True, timeout=timeout, allow_redirects=True)
                response.raise_for_status()

                with open(image_full_path, "wb") as f:
                    f.write(response.content)

                return image_full_path

            except requests.exceptions.RequestException as e:
                logging.warning(f"Attempt {attempt}: Error downloading {image_url} - {e}")
                time.sleep(5)

        logging.error(f"Failed to download image after {retries} attempts: {image_url}")
        return None

    
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
        
        # Diamond weight patterns for Fred Meyer Jewelers
        weight_patterns = [
            r'(\d+(?:\/\d+)?)\s*ct\.?\s*(?:tw\.?)?',  # "1/5 ct." or "1/2 ct. tw."
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
                if 'tw' not in text.lower() and 't.w.' not in text:
                    return f"{weight} ct tw"
                return f"{weight} ct"
        
        return "N/A"
    
    def extract_gold_type_value(self, text: str) -> str:
        """Extract gold type from text"""
        if not text:
            return "N/A"
        
        # Gold type patterns for Fred Meyer Jewelers
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