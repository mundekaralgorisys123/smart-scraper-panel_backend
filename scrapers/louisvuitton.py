import asyncio
import base64
import os
import cloudscraper
import random
import time
import uuid
import logging
from datetime import datetime
from bs4 import BeautifulSoup
import re
from typing import Dict, Any, List
import requests
from urllib.parse import urlparse, unquote, quote
from openpyxl import Workbook
from database.db_inseartin import insert_into_db, update_product_count

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

IMAGE_SAVE_PATH = os.getenv("IMAGE_SAVE_PATH")
EXCEL_DATA_PATH = os.getenv("EXCEL_DATA_PATH")


class LouisVuittonScraper:
    """Scraper for Louis Vuitton product pages with database and Excel functionality"""
    
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
            print("=================== Starting Louis Vuitton Scraper ==================")
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
            image_folder = os.path.join(self.image_save_path, f"louisvuitton_{timestamp}")
            os.makedirs(image_folder, exist_ok=True)
            
            # Create Excel file
            excel_filename = f"louisvuitton_scraped_products_{timestamp}.xlsx"
            excel_path = os.path.join(self.excel_data_path, excel_filename)
            
            # Process products
            database_records = []
            successful_downloads = 0
            
            # Create Excel workbook
            wb = Workbook()
            sheet = wb.active
            sheet.title = "Louis Vuitton Products"
            
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
                    
                    # Skip products with missing essential data
                    if self._should_skip_product(parsed_data):
                        print(f"Skipping product {i+1} due to missing essential data")
                        continue
                    
                    # Generate unique ID
                    unique_id = str(uuid.uuid4())
                    product_name = parsed_data.get('product_name', 'Unknown Product')[:495]
                    
                    # Download image using multiple strategies
                    image_url = parsed_data.get('image_url')
                    image_path = self.download_image_with_fallback(
                        image_url, product_name, image_folder, unique_id
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
                    
                    # Add delay between products to avoid rate limiting
                    if i < len(individual_products) - 1:
                        time.sleep(random.uniform(1, 3))
                    
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
                'website_type': 'louisvuitton',
                'base64_file': base64_file,
                'file_path': excel_path
            }
            
        except Exception as e:
            print(f"Error in parse_and_save_products: {e}")
            return {
                'error': str(e),
                'message': 'Failed to process products'
            }
    
    def _should_skip_product(self, parsed_data: Dict[str, Any]) -> bool:
        """Check if product should be skipped due to missing essential data"""
        product_name = parsed_data.get('product_name', 'N/A')
        price = parsed_data.get('price', 'N/A')
        image_url = parsed_data.get('image_url', 'N/A')
        
        if product_name == "N/A" and price == "N/A" and image_url == "N/A":
            return True
        return False
    
    def parse_product(self, product_html: str) -> Dict[str, Any]:
        """Parse individual product HTML using Louis Vuitton specific structure"""
        soup = BeautifulSoup(product_html, 'html.parser')
        
        return {
            'product_name': self._extract_product_name(soup),
            'price': self._extract_price(soup),
            'image_url': self._extract_image_robust(soup),
            'link': self._extract_link(soup),
            'diamond_weight': self._extract_diamond_weight(soup),
            'gold_type': self._extract_gold_type(soup),
            'badges': self._extract_badges(soup),
            'promotions': self._extract_promotions(soup)
        }
    
    def extract_individual_products_from_html(self, html_content: str) -> List[str]:
        """Extract individual product HTML blocks from Louis Vuitton HTML"""
        if not html_content:
            return []
        
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Louis Vuitton specific product selectors
        product_selectors = [
            'li.lv-product-list__item',  # Main product list items
            '.lv-product-card',  # Product card
            '[id^="lv-card-"]',  # Cards with lv-card ID
            'li[data-product-id]',  # Items with product ID
        ]
        
        individual_products = []
        
        for selector in product_selectors:
            product_tiles = soup.select(selector)
            for tile in product_tiles:
                # Skip editorial content (first item is often editorial)
                if tile.select('.lv-brand-content-module-push'):
                    continue
                individual_products.append(str(tile))
            if individual_products:
                break
        
        print(f"Found {len(individual_products)} product tiles in Louis Vuitton HTML")
        return individual_products
    
    def _extract_product_name(self, soup) -> str:
        """Extract product name from Louis Vuitton product tile"""
        name_selectors = [
            '.lv-product-card__name a',  # Product name anchor
            '.lv-product-card__name',  # Product name
            'h2.lv-product-card__name',  # Product name heading
            '.lv-product-card__title',  # Product title
            '[data-qa="product-name"]',  # QA selector for product name
        ]
        
        for selector in name_selectors:
            name_element = soup.select_one(selector)
            if name_element:
                product_name = self.clean_text(name_element.get_text())
                if product_name and product_name != "N/A":
                    return product_name
        
        return "N/A"
    
    def _extract_price(self, soup) -> str:
        """Extract price from Louis Vuitton product"""
        price_selectors = [
            '.lv-price .notranslate',  # Price with notranslate class
            '.lv-product-card__price',  # Price class
            '.lv-price',  # Price wrapper
            '.lv-product-price',  # Product price
            '[data-qa="price"]',  # QA selector for price
        ]
        
        for selector in price_selectors:
            price_element = soup.select_one(selector)
            if price_element:
                price_text = price_element.get_text(strip=True)
                # Clean up price string
                price_text = re.sub(r'\s+', ' ', price_text).strip()
                extracted_price = self.extract_price_value(price_text)
                if extracted_price != "N/A":
                    return extracted_price
        
        # Fallback: search for price patterns in the entire HTML
        html_text = soup.get_text()
        price_match = re.search(r'[₹$€]\s*\d{1,3}(?:,\d{3})*(?:\.\d{2})?', html_text)
        if price_match:
            return price_match.group(0).replace(' ', '')
        
        return "N/A"
    
    def _extract_image_robust(self, soup) -> str:
        """Extract highest resolution image from Louis Vuitton product card"""
        # Focus on the specific div structure
        front_view_div = soup.select_one('.lv-product-card__front-view.lv-product-picture')
        if not front_view_div:
            return self._extract_image_fallback(soup)
        
        # Priority 1: Get highest resolution from srcset (4096w when available)
        picture = front_view_div.find('picture')
        if picture:
            sources = picture.find_all('source')
            highest_res_url = None
            max_width = 0
            
            for source in sources:
                srcset = source.get('srcset')
                if srcset:
                    # Parse all images in srcset
                    for img_def in srcset.split(','):
                        img_def = img_def.strip()
                        if img_def:
                            parts = img_def.split()
                            if parts and 'louisvuitton.com' in parts[0]:
                                # Extract width
                                width = 0
                                if len(parts) > 1:
                                    width_match = re.search(r'(\d+)w', parts[1])
                                    if width_match:
                                        width = int(width_match.group(1))
                                
                                # Keep track of highest resolution
                                if width > max_width:
                                    max_width = width
                                    highest_res_url = parts[0]
            
            if highest_res_url:
                normalized_url = self._normalize_louisvuitton_image_url(highest_res_url)
                print(f"✅ Found highest resolution image ({max_width}w): {normalized_url}")
                return normalized_url
        
        # Priority 2: Get from noscript
        noscript = front_view_div.find('noscript')
        if noscript:
            noscript_img = noscript.find('img')
            if noscript_img and noscript_img.get('src'):
                src = noscript_img.get('src')
                if 'louisvuitton.com' in src:
                    normalized_url = self._normalize_louisvuitton_image_url(src)
                    # print(f"✅ Found image via noscript: {normalized_url}")
                    return normalized_url
        
        # Priority 3: Fallback
        return self._extract_image_fallback(soup)
    
    def _extract_image_fallback(self, soup) -> str:
        """Fallback image extraction methods"""
        # Try noscript first
        noscript = soup.find('noscript')
        if noscript:
            noscript_img = noscript.find('img')
            if noscript_img and noscript_img.get('src'):
                src = noscript_img.get('src')
                if 'louisvuitton.com' in src:
                    normalized_url = self._normalize_louisvuitton_image_url(src)
                    print(f"✅ Found image via noscript fallback: {normalized_url}")
                    return normalized_url
        
        # Try regular image extraction
        img_selectors = [
            'img.lv-smart-picture__object',
            '.lv-product-picture img',
            '.lv-product-card__front-view img',
            '.lv-product-card__media img',
        ]
        
        for selector in img_selectors:
            img_elements = soup.select(selector)
            for img_element in img_elements:
                src = img_element.get('src', '')
                if src and 'louisvuitton.com' in src and not src.startswith('data:'):
                    normalized_url = self._normalize_louisvuitton_image_url(src)
                    print(f"✅ Found image via fallback selector: {normalized_url}")
                    return normalized_url
        
        print("❌ No image found for product")
        return "N/A"
    
    def _normalize_louisvuitton_image_url(self, url: str) -> str:
        """Normalize image URL for Louis Vuitton - try different CDN approaches"""
        if not url or url == "N/A":
            return "N/A"
        
        # Ensure URL is complete
        if url.startswith('//'):
            url = f"https:{url}"
        elif url.startswith('/'):
            url = f"https://eu.louisvuitton.com{url}"
        
        # Fix URL encoding issues
        if '%20' in url or '%2520' in url:
            try:
                # Decode any double-encoded URLs
                decoded_path = unquote(url.split('images/is/image/lv/')[-1])
                # Re-encode properly
                encoded_path = quote(decoded_path, safe='')
                url = f"https://eu.louisvuitton.com/images/is/image/lv/{encoded_path}"
            except Exception as e:
                print(f"URL normalization error: {e}")
                # Fallback: simple space replacement
                url = url.replace('%2520', '%20')
        
        # Remove any existing parameters and add optimal ones
        base_url = url.split('?')[0]
        
        # Try different CDN parameter strategies
        cdn_strategies = [
            '?wid=1440&hei=1440',  # High quality
            '?wid=800&hei=800',    # Medium quality
            '?wid=600&hei=600',    # Lower quality
            '?fit=constrain,1&wid=1000&hei=1000',  # Alternative CDN parameters
        ]
        
        # Use high quality as default
        return base_url + cdn_strategies[0]
    
    def _extract_link(self, soup) -> str:
        """Extract product link from Louis Vuitton product"""
        link_selectors = [
            '.lv-product-card__name a',  # Product name link
            '.lv-product-card__url',  # Product URL
            '.lv-smart-link',  # Smart link
            'a.lv-product-card__link',  # Product card link
            '[data-qa="product-link"]',  # QA selector for product link
        ]
        
        for selector in link_selectors:
            link_element = soup.select_one(selector)
            if link_element and link_element.get('href'):
                href = link_element.get('href')
                normalized_url = self._normalize_link_url(href)
                if normalized_url != "N/A":
                    return normalized_url
        
        return "N/A"
    
    def _normalize_link_url(self, url: str) -> str:
        """Normalize link URL for Louis Vuitton"""
        if not url or url == "N/A":
            return "N/A"
        if url.startswith('http'):
            return url
        elif url.startswith('//'):
            return f"https:{url}"
        elif url.startswith('/'):
            return f"https://eu.louisvuitton.com{url}"
        return url
    
    def _extract_diamond_weight(self, soup) -> str:
        """Extract diamond weight from product name and description"""
        product_name = self._extract_product_name(soup)
        description = soup.get_text()
        return self.extract_diamond_weight_value(product_name + " " + description)
    
    def _extract_gold_type(self, soup) -> str:
        """Extract gold type from product name"""
        product_name = self._extract_product_name(soup)
        description = soup.get_text()
        return self.extract_gold_type_value(product_name + " " + description)
    
    def _extract_badges(self, soup) -> list:
        """Extract badges and additional info from Louis Vuitton product"""
        badges = []
        
        # Extract product labels/tags
        label_selectors = [
            '.lv-product-card-label span',  # Product label spans
            '.lv-product-card__feature',  # Product features
            '.lv-product-badge',  # Product badges
            '.lv-product-tag',  # Product tags
        ]
        
        for selector in label_selectors:
            label_elements = soup.select(selector)
            for label in label_elements:
                label_text = self.clean_text(label.get_text())
                if label_text and label_text not in badges and len(label_text) < 100:
                    badges.append(label_text)
        
        return badges
    
    def _extract_promotions(self, soup) -> str:
        """Extract promotion text from Louis Vuitton product"""
        promotions = []
        
        # Look for wishlist elements
        wishlist_selectors = [
            '.lv-product-add-to-wishlist',  # Wishlist button
            '[aria-label*="wishlist"]',  # Wishlist aria labels
            '.lv-wishlist',  # Wishlist
        ]
        
        for selector in wishlist_selectors:
            if soup.select(selector):
                promotions.append("Available for Wishlist")
                break
        
        # Look for other promotional elements
        promo_selectors = [
            '.lv-product-promo',  # Product promotions
            '.lv-special-offer',  # Special offers
            '.lv-exclusive',  # Exclusive items
        ]
        
        for selector in promo_selectors:
            promo_elements = soup.select(selector)
            for promo in promo_elements:
                promo_text = self.clean_text(promo.get_text())
                if promo_text and promo_text not in promotions:
                    promotions.append(promo_text)
        
        return " | ".join(promotions) if promotions else "N/A"
    
    def download_image_with_fallback(self, image_url: str, product_name: str, image_folder: str, unique_id: str) -> str:
        """
        Try multiple download methods with fallback strategies
        """
        if not image_url or image_url == "N/A":
            return "N/A"
        
        methods = [
            self.download_with_simple_requests,
        ]
        
        for method in methods:
            try:
                image_path = method(image_url, product_name, image_folder, unique_id)
                if image_path != "N/A":
                    return image_path
            except Exception as e:
                logger.warning(f"Method {method.__name__} failed: {e}")
                continue
        
        return "N/A"
    

    def download_with_simple_requests(self, image_url: str, product_name: str, image_folder: str, unique_id: str) -> str:
        """Simple requests approach as last resort"""
        try:
            image_path = os.path.join(image_folder, f"{unique_id}.jpg")
            
            # Very simple request
            response = requests.get(
                image_url,
                timeout=30,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Accept": "image/*",
                }
            )
            
            if response.status_code == 200:
                with open(image_path, "wb") as f:
                    f.write(response.content)
                
                if os.path.exists(image_path) and os.path.getsize(image_path) > 1000:
                    logger.info(f"[SimpleRequests] ✅ Downloaded {product_name}")
                    return image_path
                    
        except Exception as e:
            logger.warning(f"[SimpleRequests] Failed: {e}")
        
        return "N/A"
    
    def _fix_louisvuitton_url(self, url: str) -> str:
        """Fix Louis Vuitton URL encoding and parameters"""
        if not url or url == "N/A":
            return "N/A"
        
        # Ensure proper protocol
        if url.startswith('//'):
            url = f"https:{url}"
        elif url.startswith('/'):
            url = f"https://eu.louisvuitton.com{url}"
        
        # Fix URL encoding issues
        if '%20' in url or '%2520' in url:
            try:
                # Extract the path after /images/is/image/lv/
                path_parts = url.split('images/is/image/lv/')
                if len(path_parts) > 1:
                    decoded_path = unquote(path_parts[1])
                    # Re-encode properly
                    encoded_path = quote(decoded_path, safe='')
                    url = f"https://eu.louisvuitton.com/images/is/image/lv/{encoded_path}"
            except Exception as e:
                print(f"URL fixing error: {e}")
                # Fallback: simple space replacement
                url = url.replace('%2520', '%20')
        
        # Remove any existing query parameters and add optimal ones
        base_url = url.split('?')[0]
        
        # Use high quality parameters
        return base_url + '?wid=1440&hei=1440'
    
    def _get_file_extension_from_content_type(self, content_type: str) -> str:
        """Get appropriate file extension from content type"""
        content_type = content_type.lower()
        
        if 'avif' in content_type:
            return '.avif'
        elif 'webp' in content_type:
            return '.webp'
        elif 'png' in content_type:
            return '.png'
        elif 'jpeg' in content_type or 'jpg' in content_type:
            return '.jpg'
        elif 'gif' in content_type:
            return '.gif'
        elif 'svg' in content_type:
            return '.svg'
        else:
            return '.jpg'  # Fallback

    def _clean_filename(self, filename: str) -> str:
        """Clean filename to remove invalid characters"""
        if not filename:
            return "unknown"
        # Remove invalid filename characters
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            filename = filename.replace(char, '_')
        # Remove multiple spaces and trim
        filename = re.sub(r'\s+', ' ', filename).strip()
        # Limit filename length
        if len(filename) > 100:
            filename = filename[:100]
        return filename

    def extract_price_value(self, text: str) -> str:
        """Extract price from text"""
        if not text:
            return "N/A"
        
        # Louis Vuitton price formats
        price_patterns = [
            r'[₹$€]\s*\d{1,3}(?:,\d{2,3})*(?:\.\d{2})?',  # Standard format
            r'[₹$€]\s*\d{1,3}(?:\.\d{3})*(?:,\d{2})?',    # European format
            r'[₹$€]\s*\d+(?:\.\d{2})?',                   # Simple format
        ]
        
        for pattern in price_patterns:
            price_match = re.search(pattern, text)
            if price_match:
                price = price_match.group(0).replace(' ', '')
                return price
        
        return "N/A"
    
    def clean_text(self, text: str) -> str:
        """Clean and normalize text"""
        if not text:
            return ""
        # Remove extra whitespace and normalize
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
                if 'tw' not in text.lower() and 'ctw' not in text.lower():
                    return f"{weight} ct"
                return f"{weight} ct"
        
        return "N/A"
    
    def extract_gold_type_value(self, text: str) -> str:
        """Extract gold type from text"""
        if not text:
            return "N/A"
        
        gold_patterns = [
            r'\b(White Gold|Yellow Gold|Rose Gold|Pink Gold)\b',
            r'\b(\d{1,2}K)\s*(?:White|Yellow|Rose|Pink)?\s*Gold\b',
            r'\b(Platinum|Gold|Silver)\b',
            r'\b(\d{1,2}K)\b',
        ]
        
        for pattern in gold_patterns:
            gold_match = re.search(pattern, text, re.IGNORECASE)
            if gold_match:
                gold_type = gold_match.group(1)
                return gold_type.title()
        
        return "N/A"