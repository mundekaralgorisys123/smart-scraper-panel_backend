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


class VanCleefArpelsScraper:
    """Scraper for Van Cleef & Arpels product pages with database and Excel functionality"""
    
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
            print("=================== Starting Van Cleef & Arpels Scraper ==================")
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
            image_folder = os.path.join(self.image_save_path, f"vancleefarpels_{timestamp}")
            os.makedirs(image_folder, exist_ok=True)
            
            # Create Excel file
            excel_filename = f"vancleefarpels_scraped_products_{timestamp}.xlsx"
            excel_path = os.path.join(self.excel_data_path, excel_filename)
            
            # Process products
            database_records = []
            successful_downloads = 0
            
            # Create Excel workbook
            wb = Workbook()
            sheet = wb.active
            sheet.title = "Van Cleef & Arpels Products"
            
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
                'website_type': 'vancleefarpels',
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
        """Parse individual product HTML using Van Cleef & Arpels specific structure"""
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
        """Extract individual product HTML blocks from Van Cleef & Arpels HTML"""
        if not html_content:
            return []
        
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Van Cleef & Arpels specific product selectors
        product_selectors = [
            'li.vca-srl-product-tile',  # Main product list item
            'article.vca-pl-product',  # Product article
            'li[data-page="true"]',  # Data page items
            '.results-list li',  # Results list items
        ]
        
        individual_products = []
        
        for selector in product_selectors:
            product_tiles = soup.select(selector)
            for tile in product_tiles:
                individual_products.append(str(tile))
            if individual_products:
                break
        
        print(f"Found {len(individual_products)} product tiles in Van Cleef & Arpels HTML")
        return individual_products
    
    def _extract_product_name(self, soup) -> str:
        """Extract product name from Van Cleef & Arpels product tile"""
        # Extract main product name from h2 tag
        name_element = soup.select_one('h2.product-name.vca-product-list-01')
        if name_element:
            product_name = self.clean_text(name_element.get_text())
            
            # Add description to product name (matching your Playwright code)
            desc_element = soup.select_one('p.product-description.vca-body-02.vca-text-center')
            if desc_element:
                desc_text = self.clean_text(desc_element.get_text())
                if desc_text:
                    product_name += f" - {desc_text}"
            
            return product_name
        
        # Fallback to link text
        link_element = soup.select_one('a.vca-srl-ref-link')
        if link_element:
            return self.clean_text(link_element.get_text())
        
        return "N/A"
    
    def _extract_price(self, soup) -> str:
        """Extract price from Van Cleef & Arpels product"""
        price_element = soup.select_one('span.vca-price')
        if price_element:
            price_text = price_element.get_text(strip=True)
            return self.extract_price_value(price_text)
        
        # Look for price in any text
        price_match = re.search(r'\$\s*\d{1,3}(?:,\d{3})*(?:\.\d{2})?', soup.get_text())
        if price_match:
            return price_match.group(0).replace(' ', '')  # Remove spaces from price
        
        return "N/A"
    
    def _extract_image(self, soup) -> str:
        """Extract image URL from Van Cleef & Arpels product"""
        # Van Cleef & Arpels specific image selectors
        img_selectors = [
            'div.image-container img',  # Main image container
            '.swiper-slide-active img',  # Active swiper slide image
            '.product-tile img',  # Product tile image
            'img[src*="vancleefarpels.com"]',  # Van Cleef & Arpels domain images
        ]
        
        for selector in img_selectors:
            img_element = soup.select_one(selector)
            if img_element and img_element.get('src'):
                image_src = img_element.get('src')
                return image_src  # Return relative URL for modification later
        
        return "N/A"
    
    def _extract_link(self, soup) -> str:
        """Extract product link from Van Cleef & Arpels product"""
        link_selectors = [
            'a.vca-srl-ref-link',  # Main product link
            'a[href*="/collections/"]',  # Collection links
            'a[data-reference]',  # Links with data reference
            '.vca-swiper-pdp-link',  # Swiper PDP links
        ]
        
        for selector in link_selectors:
            link_element = soup.select_one(selector)
            if link_element and link_element.get('href'):
                href = link_element.get('href')
                return self._normalize_link_url(href)
        
        return "N/A"
    
    def _extract_diamond_weight(self, soup) -> str:
        """Extract diamond weight from product name and description"""
        product_name = self._extract_product_name(soup)
        
        # Try to extract from product name first
        diamond_weight = self.extract_diamond_weight_value(product_name)
        if diamond_weight != "N/A":
            return diamond_weight
        
        # Try to extract from data attributes
        product_tile = soup.select_one('div[data-vue-stats-product]')
        if product_tile and product_tile.get('data-vue-stats-product'):
            try:
                import json
                stats_data = product_tile.get('data-vue-stats-product')
                product_stats = json.loads(stats_data)
                
                # Look for diamond information in stats
                if 'item_name' in product_stats:
                    item_name = product_stats['item_name']
                    diamond_match = re.search(r'(\d+(?:\.\d+)?)\s*carats', item_name, re.IGNORECASE)
                    if diamond_match:
                        return f"{diamond_match.group(1)} ct"
            except:
                pass
        
        return "N/A"
    
    def _extract_gold_type(self, soup) -> str:
        """Extract gold type from product description and data attributes"""
        # First try to extract from product description
        desc_element = soup.select_one('p.product-description.vca-body-02.vca-text-center')
        if desc_element:
            desc_text = self.clean_text(desc_element.get_text())
            gold_type = self.extract_gold_type_value(desc_text)
            if gold_type != "N/A":
                return gold_type
        
        # Try to extract from product name
        product_name = self._extract_product_name(soup)
        gold_type = self.extract_gold_type_value(product_name)
        if gold_type != "N/A":
            return gold_type
        
        # Try to extract from data attributes
        product_tile = soup.select_one('div[data-vue-stats-product]')
        if product_tile and product_tile.get('data-vue-stats-product'):
            try:
                import json
                stats_data = product_tile.get('data-vue-stats-product')
                product_stats = json.loads(stats_data)
                
                # Extract material from stats
                if 'item_material_jewelry' in product_stats:
                    material = product_stats['item_material_jewelry']
                    if material and material != "N/A":
                        return material
            except:
                pass
        
        return "N/A"
    
    def _extract_badges(self, soup) -> list:
        """Extract badges and additional info from Van Cleef & Arpels product"""
        badges = []
        
        # Extract collection information from data attributes
        product_tile = soup.select_one('div[data-vue-stats-product]')
        if product_tile and product_tile.get('data-vue-stats-product'):
            try:
                import json
                stats_data = product_tile.get('data-vue-stats-product')
                product_stats = json.loads(stats_data)
                
                # Add collection as badge
                if 'item_collection' in product_stats:
                    collection = product_stats['item_collection']
                    if collection and collection != "N/A" and collection != "White diamond High Jewelry":
                        badges.append(collection)
                
                # Add line as badge
                if 'item_line' in product_stats:
                    line = product_stats['item_line']
                    if line and line != "N/A" and line != "HIGH_JEWELRY":
                        badges.append(line.replace('_', ' ').title())
            except:
                pass
        
        # Extract special edition info
        special_edition = soup.select_one('[class*="special"]')
        if special_edition:
            badge_text = self.clean_text(special_edition.get_text())
            if badge_text:
                badges.append(badge_text)
        
        return badges
    
    def _extract_promotions(self, soup) -> str:
        """Extract promotion text from Van Cleef & Arpels product"""
        # Van Cleef & Arpels doesn't seem to have prominent promotions
        # Look for any promotional elements
        promo_selectors = [
            '[class*="promo"]',
            '[class*="sale"]',
            '[class*="new"]',
            '.on-demand',  # On demand products
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
        """Normalize image URL for Van Cleef & Arpels"""
        if not url or url == "N/A":
            return "N/A"
        if url.startswith('http'):
            return url
        elif url.startswith('//'):
            return f"https:{url}"
        elif url.startswith('/'):
            return f"https://www.vancleefarpels.com{url}"
        return url
    
    def _normalize_link_url(self, url: str) -> str:
        """Normalize link URL for Van Cleef & Arpels"""
        if not url or url == "N/A":
            return "N/A"
        if url.startswith('http'):
            return url
        elif url.startswith('//'):
            return f"https:{url}"
        elif url.startswith('/'):
            return f"https://www.vancleefarpels.com{url}"
        return url

    def modify_image_url(self, image_url):
        """
        Get the highest quality version of Van Cleef & Arpels images.
        """
        if not image_url or image_url == "N/A":
            return image_url

        print(f"Original image URL: {image_url}")
        
        # First normalize the URL to absolute format
        normalized_url = self._normalize_image_url(image_url)
        print(f"Normalized URL: {normalized_url}")
        
        # Strategy 1: Try to get the original image without any transforms
        if '.transform.' in normalized_url:
            # Remove everything after the original file extension including .transform
            # Pattern: /path/to/image.jpeg.transform.vca-w350-1x.png
            if '.jpeg.transform.' in normalized_url:
                original_url = normalized_url.split('.jpeg.transform.')[0] + '.jpeg'
                print(f"Strategy 1 - Original JPEG: {original_url}")
                return original_url
            elif '.jpg.transform.' in normalized_url:
                original_url = normalized_url.split('.jpg.transform.')[0] + '.jpg'
                print(f"Strategy 1 - Original JPG: {original_url}")
                return original_url
            elif '.png.transform.' in normalized_url:
                original_url = normalized_url.split('.png.transform.')[0] + '.png'
                print(f"Strategy 1 - Original PNG: {original_url}")
                return original_url
        
        # Strategy 2: If we can't get the original, try to get the highest quality transform
        if 'transform.vca-w' in normalized_url:
            # Replace with larger size (from w350 to w1200)
            high_quality_url = re.sub(r'transform\.vca-w\d+-\d+x', 'transform.vca-w1200-1x', normalized_url)
            print(f"Strategy 2 - High quality: {high_quality_url}")
            return high_quality_url
        
        print(f"Final - Using normalized: {normalized_url}")
        return normalized_url

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
                        "Referer": "https://www.vancleefarpels.com/",
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
        
        # Van Cleef & Arpels price format: "$ 905,000" with space
        price_match = re.search(r'\$\s*\d{1,3}(?:,\d{3})*(?:\.\d{2})?', text)
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
            r'(\d+)\s*stones',  # For Van Cleef & Arpels format
        ]
        
        for pattern in weight_patterns:
            weight_match = re.search(pattern, text, re.IGNORECASE)
            if weight_match:
                weight = weight_match.group(1)
                if 'carat' in text.lower() or 'stones' in text.lower():
                    return f"{weight} ct"
                elif 'tw' not in text.lower():
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