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


class ChaumetScraper:
    """Scraper for Chaumet product pages with database and Excel functionality"""
    
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
            print("=================== Starting Chaumet Scraper ==================")
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
            image_folder = os.path.join(self.image_save_path, f"chaumet_{timestamp}")
            os.makedirs(image_folder, exist_ok=True)
            
            # Create Excel file
            excel_filename = f"chaumet_scraped_products_{timestamp}.xlsx"
            excel_path = os.path.join(self.excel_data_path, excel_filename)
            
            # Process products
            database_records = []
            successful_downloads = 0
            
            # Create Excel workbook
            wb = Workbook()
            sheet = wb.active
            sheet.title = "Chaumet Products"
            
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
                'website_type': 'chaumet',
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
        """Parse individual product HTML using Chaumet specific structure"""
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
        """Extract individual product HTML blocks from Chaumet HTML"""
        if not html_content:
            return []
        
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Chaumet specific product selectors
        product_selectors = [
            'li.item',  # Main product list items
            '.c-product-card',  # Product card container
            '.product-items .item',  # Product items
        ]
        
        individual_products = []
        
        for selector in product_selectors:
            product_tiles = soup.select(selector)
            for tile in product_tiles:
                # Skip non-product items (like gift guide push items)
                if not tile.select('.c-product-card'):
                    continue
                individual_products.append(str(tile))
            if individual_products:
                break
        
        print(f"Found {len(individual_products)} product tiles in Chaumet HTML")
        return individual_products
    
    def _extract_product_name(self, soup) -> str:
        """Extract product name from Chaumet product tile by combining name and description"""
        # Extract main product name
        name_selectors = [
            'a.product__name span:first-child',  # Main product name span
            '.product__name',  # Product name link
            '[data-product-element="name"]',  # Data attribute
        ]
        
        main_name = "N/A"
        for selector in name_selectors:
            name_element = soup.select_one(selector)
            if name_element:
                main_name = self.clean_text(name_element.get_text())
                break
        
        # Extract description to get materials
        desc_selectors = [
            '.c-product-card__title-second',  # Description span
            '.product__description',  # Description class
        ]
        
        description = "N/A"
        for selector in desc_selectors:
            desc_element = soup.select_one(selector)
            if desc_element:
                description = self.clean_text(desc_element.get_text())
                break
        
        # Combine name and description to create full product name
        if main_name != "N/A" and description != "N/A":
            # Format: "Joséphine Éclat Floral 2-carat solitaire - Platinum, yellow diamond, diamonds"
            full_name = f"{main_name} - {description}"
        elif main_name != "N/A":
            full_name = main_name
        elif description != "N/A":
            full_name = description
        else:
            full_name = "Unknown Product"
        
        return full_name[:495]  # Truncate to fit database field
    
    def _extract_price(self, soup) -> str:
        """Extract price from Chaumet product"""
        price_selectors = [
            '.price-wrapper .price',  # Price wrapper
            '.c-product-card__price .price',  # Price in card
            '[data-price-type="finalPrice"]',  # Data attribute for price
            '.price-container .price',  # Price container
        ]
        
        for selector in price_selectors:
            price_element = soup.select_one(selector)
            if price_element:
                price_text = price_element.get_text(strip=True)
                return self.extract_price_value(price_text)
        
        # Check for "Price on demand"
        demand_selectors = [
            '.t-primary-text.u-fz-11.u-grey-opacity',  # Price on demand text
            '.price-box .t-primary-text',  # Price box text
        ]
        
        for selector in demand_selectors:
            demand_element = soup.select_one(selector)
            if demand_element and "price on demand" in demand_element.get_text(strip=True).lower():
                return "Price on demand"
        
        return "N/A"
    
    def _extract_image(self, soup) -> str:
        """Extract image URL from Chaumet product"""
        # Chaumet specific image selectors
        img_selectors = [
            '.slick-slide img.lazyload',  # Lazy loaded images in slider
            '.product__thumbnail img.lazyload',  # Product thumbnail images
            'img.lazyload',  # Any lazy loaded image
            'img[data-src]',  # Images with data-src
        ]
        
        for selector in img_selectors:
            img_element = soup.select_one(selector)
            if img_element:
                # Try data-src first for lazy loading
                image_url = img_element.get('data-src')
                if image_url:
                    return self._normalize_chaumet_image_url(image_url)
                
                # Fallback to src attribute
                image_url = img_element.get('src')
                if image_url:
                    return self._normalize_chaumet_image_url(image_url)
        
        return "N/A"
    
    def _normalize_chaumet_image_url(self, url: str) -> str:
        """Normalize image URL for Chaumet"""
        if not url or url == "N/A":
            return "N/A"
        
        # Ensure URL is complete
        if url.startswith('//'):
            url = f"https:{url}"
        elif url.startswith('/') and 'chaumet.com' not in url:
            url = f"https://www.chaumet.com{url}"
        
        # Enhance image quality if possible
        if 'chaumet.com' in url:
            # Remove size restrictions for higher quality
            url = re.sub(r'\?w=\d+&h=\d+', '?w=800&h=800', url)
            url = re.sub(r'\?w=\d+', '?w=800', url)
        
        return url
    
    def _extract_link(self, soup) -> str:
        """Extract product link from Chaumet product"""
        link_selectors = [
            'a.product__name',  # Product name link
            '.c-product-card a[href*="/us_en/"]',  # Links with US English path
            'a[href*="chaumet.com"]',  # Any Chaumet link
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
        return self.extract_diamond_weight_value(product_name)
    
    def _extract_gold_type(self, soup) -> str:
        """Extract gold type from product description"""
        # Extract from description in title second
        desc_selectors = [
            '.c-product-card__title-second',  # Description span
            '.product__description',  # Description class
        ]
        
        for selector in desc_selectors:
            desc_element = soup.select_one(selector)
            if desc_element:
                description = self.clean_text(desc_element.get_text())
                gold_type = self.extract_gold_type_value(description)
                if gold_type != "N/A":
                    return gold_type
        
        # Also check product name
        product_name = self._extract_product_name(soup)
        return self.extract_gold_type_value(product_name)
    
    def _extract_badges(self, soup) -> list:
        """Extract badges and additional info from Chaumet product"""
        badges = []
        
        # Extract carat tags (e.g., "FROM 2 CARATS")
        tag_selectors = [
            'span.u-gold-light',  # Gold colored tags
            '.product__tag span',  # Product tag spans
        ]
        
        for selector in tag_selectors:
            tag_elements = soup.select(selector)
            for tag in tag_elements:
                tag_text = self.clean_text(tag.get_text())
                if tag_text and tag_text not in badges:
                    badges.append(tag_text)
        
        # Extract price demand info
        demand_selectors = [
            '.t-primary-text.u-fz-11.u-grey-opacity',  # Price on demand
            '.price-box div',  # Price box content
        ]
        
        for selector in demand_selectors:
            demand_elements = soup.select(selector)
            for demand in demand_elements:
                demand_text = self.clean_text(demand.get_text())
                if "price on demand" in demand_text.lower() and demand_text not in badges:
                    badges.append(demand_text)
        
        return badges
    
    def _extract_promotions(self, soup) -> str:
        """Extract promotion text from Chaumet product"""
        # Look for special icons or badges
        icon_selectors = [
            '.card__top-left svg',  # Top left icons
            '[class*="diamond"]',  # Diamond icons
        ]
        
        promotions = []
        for selector in icon_selectors:
            icon_elements = soup.select(selector)
            if icon_elements:
                promotions.append("Diamond Collection")
                break
        
        return " | ".join(promotions) if promotions else "N/A"
    
    def _normalize_link_url(self, url: str) -> str:
        """Normalize link URL for Chaumet"""
        if not url or url == "N/A":
            return "N/A"
        if url.startswith('http'):
            return url
        elif url.startswith('//'):
            return f"https:{url}"
        elif url.startswith('/'):
            return f"https://www.chaumet.com{url}"
        return url

    def download_image(self, image_url: str, product_name: str, timestamp: str, 
                      image_folder: str, unique_id: str, retries: int = 3) -> str:
        """Synchronous image download method with enhanced error handling"""
        if not image_url or image_url == "N/A":
            return "N/A"

        # Clean filename
        image_filename = f"{unique_id}_{timestamp}.jpg"
        image_full_path = os.path.join(image_folder, image_filename)
        
        # Modify the image URL to get higher quality
        modified_url = self._normalize_chaumet_image_url(image_url)
        
        for attempt in range(retries):
            try:
                response = requests.get(
                    modified_url,
                    timeout=45,
                    headers={
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128 Safari/537.36",
                        "Referer": "https://www.chaumet.com/",
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
        
        # Chaumet price formats: "$9,750.00", "Price on demand"
        if "price on demand" in text.lower():
            return "Price on demand"
        
        price_match = re.search(r'\$?\s*\d{1,3}(?:,\d{3})*(?:\.\d{2})?', text)
        if price_match:
            price = price_match.group(0)
            if not price.startswith('$'):
                price = f"${price}"
            return price
        
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
            r'(\d+(?:\.\d+)?)\s*cts?\s*solitaire',
            r'(\d+(?:\.\d+)?)\s*cts?',
            r'(\d+(?:\.\d+)?)\s*carat',
            r'(\d+(?:\.\d+)?)\s*carats',
            r'(\d+(?:\.\d+)?)-carat',
        ]
        
        for pattern in weight_patterns:
            weight_match = re.search(pattern, text, re.IGNORECASE)
            if weight_match:
                weight = weight_match.group(1)
                return f"{weight} ct"
        
        return "N/A"
    
    def extract_gold_type_value(self, text: str) -> str:
        """Extract gold type from text"""
        if not text:
            return "N/A"
        
        gold_patterns = [
            r'(Platinum)',
            r'(White Gold|Yellow Gold|Rose Gold)',
            r'(\d+k)\s*(?:White|Yellow|Rose)?\s*Gold',
        ]
        
        for pattern in gold_patterns:
            gold_match = re.search(pattern, text, re.IGNORECASE)
            if gold_match:
                gold_type = gold_match.group(1)
                return gold_type.title()
        
        return "N/A"