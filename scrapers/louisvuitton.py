import asyncio
import base64
import os
from random import random
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
from database_quey.db_inseartin import insert_into_db, update_product_count

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
            'image_url': self._extract_image_robust(soup),  # Use robust method
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
        """More robust image extraction for Louis Vuitton"""
        # Method 1: Try to find noscript fallback (often contains direct image)
        noscript = soup.find('noscript')
        if noscript:
            noscript_img = noscript.find('img')
            if noscript_img and noscript_img.get('src'):
                src = noscript_img.get('src')
                if 'louisvuitton.com' in src:
                    normalized_url = self._normalize_louisvuitton_image_url(src)
                    print(f"Found image via noscript: {normalized_url}")
                    return normalized_url
        
        # Method 2: Parse all picture sources
        pictures = soup.find_all('picture')
        for picture in pictures:
            sources = picture.find_all('source')
            for source in sources:
                srcset = source.get('srcset') or source.get('data-srcset')
                if srcset:
                    # Get the highest resolution image from srcset
                    images = []
                    for img_def in srcset.split(','):
                        img_def = img_def.strip()
                        if img_def:
                            parts = img_def.split()
                            if parts and 'louisvuitton.com' in parts[0] and not parts[0].startswith('data:'):
                                width = 0
                                if len(parts) > 1:
                                    width_match = re.search(r'(\d+)w', parts[1])
                                    if width_match:
                                        width = int(width_match.group(1))
                                images.append((parts[0], width))
                    
                    if images:
                        images.sort(key=lambda x: x[1], reverse=True)
                        best_image_url = images[0][0]
                        normalized_url = self._normalize_louisvuitton_image_url(best_image_url)
                        print(f"Found image via picture source: {normalized_url}")
                        return normalized_url
        
        # Method 3: Regular image extraction
        return self._extract_image(soup)
    
    def _extract_image(self, soup) -> str:
        """Extract image URL from Louis Vuitton product"""
        # Louis Vuitton specific image selectors
        img_selectors = [
            'img.lv-smart-picture__object',  # Smart picture object
            '.lv-product-picture img',  # Product picture
            '.lv-product-card__front-view img',  # Front view image
            '.lv-mini-slider img',  # Mini slider images
            '.lv-product-card__media img',  # Product media images
        ]
        
        for selector in img_selectors:
            img_elements = soup.select(selector)
            for img_element in img_elements:
                # Skip base64 placeholder images
                src = img_element.get('src', '')
                if src and src.startswith('data:image/gif;base64'):
                    continue
                    
                # Try to get the highest resolution image from srcset
                srcset = img_element.get('srcset') or img_element.get('data-srcset')
                if srcset:
                    # Parse srcset to get all available images
                    image_sources = []
                    for source in srcset.split(','):
                        source = source.strip()
                        if source:
                            parts = source.split()
                            if len(parts) >= 1:
                                url = parts[0]
                                if url and 'louisvuitton.com' in url and not url.startswith('data:'):
                                    # Extract width if available to prioritize higher resolution
                                    width = 0
                                    if len(parts) >= 2:
                                        width_match = re.search(r'(\d+)w', parts[1])
                                        if width_match:
                                            width = int(width_match.group(1))
                                    image_sources.append((url, width))
                    
                    if image_sources:
                        # Sort by width (highest first) and return the highest resolution
                        image_sources.sort(key=lambda x: x[1], reverse=True)
                        best_image_url = image_sources[0][0]
                        normalized_url = self._normalize_louisvuitton_image_url(best_image_url)
                        print(f"Found image via srcset: {normalized_url}")
                        return normalized_url
                
                # Fallback to src attribute if it's not a placeholder
                if src and 'louisvuitton.com' in src and not src.startswith('data:'):
                    normalized_url = self._normalize_louisvuitton_image_url(src)
                    print(f"Found image via src: {normalized_url}")
                    return normalized_url
        
        print("No image found for product")
        return "N/A"
    
    def _normalize_louisvuitton_image_url(self, url: str) -> str:
        """Normalize image URL for Louis Vuitton - enhance quality"""
        if not url or url == "N/A":
            return "N/A"
        
        # Ensure URL is complete
        if url.startswith('//'):
            url = f"https:{url}"
        elif url.startswith('/'):
            url = f"https://in.louisvuitton.com{url}"
        
        # Enhance image quality - get maximum resolution
        if 'louisvuitton.com' in url and 'images/is/image' in url:
            # Remove existing size parameters but keep the base URL
            url = re.sub(r'\?.*$', '', url)
            # Add parameters for highest quality
            url += '?wid=2000&hei=2000'
        
        return url
    
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
            return f"https://in.louisvuitton.com{url}"
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
    
    def download_image(self, image_url: str, product_name: str, timestamp: str, 
                      image_folder: str, unique_id: str, retries: int = 3) -> str:
        """Synchronous image download method with enhanced error handling for Louis Vuitton"""
        if not image_url or image_url == "N/A":
            return "N/A"

        # Clean filename
        image_filename = f"{unique_id}_{timestamp}.jpg"
        image_full_path = os.path.join(image_folder, image_filename)
        
        print(f"Downloading image for: {product_name}")
        print(f"Image URL: {image_url}")
        
        for attempt in range(retries):
            try:
                user_agents = [
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                ]
                
                headers = {
                    "User-Agent": random.choice(user_agents),
                    "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept-Encoding": "gzip, deflate, br",
                    "Referer": "https://in.louisvuitton.com/",
                    "Sec-Fetch-Dest": "image",
                    "Sec-Fetch-Mode": "no-cors",
                    "Sec-Fetch-Site": "same-origin",
                    "Cache-Control": "no-cache",
                    "Pragma": "no-cache",
                    "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
                    "Sec-Ch-Ua-Mobile": "?0",
                    "Sec-Ch-Ua-Platform": '"Windows"'
                }
                
                response = requests.get(
                    image_url,
                    headers=headers,
                    timeout=30,
                    stream=True,
                    allow_redirects=True
                )
                response.raise_for_status()
                
                # Verify it's actually an image
                content_type = response.headers.get('content-type', '')
                if not content_type.startswith('image/'):
                    logger.warning(f"URL returned non-image content type: {content_type}")
                    if 'text/html' in content_type:
                        logger.warning("Server returned HTML instead of image, likely blocked")
                        continue
                
                # Get file size
                content_length = response.headers.get('content-length')
                if content_length and int(content_length) < 1000:
                    logger.warning(f"Image too small ({content_length} bytes), likely error page")
                    continue
                
                # Download the image
                with open(image_full_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                
                # Verify the downloaded file
                file_size = os.path.getsize(image_full_path)
                if file_size < 1000:
                    logger.warning(f"Downloaded file too small ({file_size} bytes), deleting")
                    os.remove(image_full_path)
                    continue
                
                logger.info(f"Successfully downloaded image for {product_name} ({file_size} bytes)")
                return image_full_path
                
            except requests.RequestException as e:
                logger.warning(f"Retry {attempt + 1}/{retries} - Error downloading {product_name}: {e}")
                if attempt < retries - 1:
                    time.sleep(2)
            except Exception as e:
                logger.warning(f"Retry {attempt + 1}/{retries} - Unexpected error: {e}")
                if attempt < retries - 1:
                    time.sleep(2)
        
        logger.error(f"Failed to download {product_name} after {retries} attempts.")
        return "N/A"
    
    def extract_price_value(self, text: str) -> str:
        """Extract price from text"""
        if not text:
            return "N/A"
        
        # Louis Vuitton price formats: "₹14,60,000.00", "₹2,21,000.00", "$1,500.00", "€1.200,00"
        price_patterns = [
            r'[₹$€]\s*\d{1,3}(?:,\d{2,3})*(?:\.\d{2})?',  # Standard format
            r'[₹$€]\s*\d{1,3}(?:\.\d{3})*(?:,\d{2})?',    # European format
            r'[₹$€]\s*\d+(?:\.\d{2})?',                   # Simple format
        ]
        
        for pattern in price_patterns:
            price_match = re.search(pattern, text)
            if price_match:
                price = price_match.group(0).replace(' ', '')
                # Ensure proper formatting
                if '₹' in price:
                    return price
                elif '$' in price:
                    return price
                elif '€' in price:
                    return price
                else:
                    return f"₹{price}"  # Default to INR if no currency symbol
        
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
            r'(\d+(?:\/\d+)?)\s*ct',
            r'(\d+(?:\.\d+)?)\s*diamond',
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