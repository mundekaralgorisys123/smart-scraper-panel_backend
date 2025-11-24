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


class MacysParser:
    """Parser for Macy's product pages with database and Excel functionality"""
    
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
            print("=================== Starting Macy's Parser ==================")
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
            image_folder = os.path.join(self.image_save_path, f"macys_{timestamp}")
            os.makedirs(image_folder, exist_ok=True)
            
            # Create Excel file
            excel_filename = f"macys_scraped_products_{timestamp}.xlsx"
            excel_path = os.path.join(self.excel_data_path, excel_filename)
            
            # Process products
            database_records = []
            successful_downloads = 0
            
            # Create Excel workbook
            wb = Workbook()
            sheet = wb.active
            sheet.title = "Macy's Products"
            
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
                'website_type': 'macys',
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
        """Extract individual product HTML blocks from Macy's HTML"""
        if not html_content:
            return []
        
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # DEBUG: Print HTML structure to understand what we're working with
        print("=== DEBUG HTML ANALYSIS ===")
        # print(f"HTML content length: {len(html_content)}")
        
        # Look specifically for the structure you provided
        # print("\n=== LOOKING FOR SPECIFIC MACY'S STRUCTURE ===")
        
        # Find list items with the specific class pattern from your HTML
        list_items = soup.find_all('li', class_=re.compile(r'cell.*sortablegrid-product'))
        # print(f"Found {len(list_items)} list items with 'cell sortablegrid-product' classes")
        
        # Find elements with data-liindex attribute (your HTML has this)
        liindex_items = soup.find_all(attrs={'data-liindex': True})
        # print(f"Found {len(liindex_items)} elements with data-liindex attribute")
        
        # Find product thumbnail containers
        thumbnail_containers = soup.find_all(class_='product-thumbnail-container')
        # print(f"Found {len(thumbnail_containers)} product-thumbnail-container elements")
        
        # Find elements with product descriptions
        product_descriptions = soup.find_all(class_='product-description')
        # print(f"Found {len(product_descriptions)} product-description elements")
        
        # Find pricing elements
        pricing_elements = soup.find_all(class_='pricing')
        # print(f"Found {len(pricing_elements)} pricing elements")
        
        individual_products = []
        
        # STRATEGY 1: Use the specific structure from your HTML
        if list_items:
            print("Using strategy 1: list items with specific classes")
            for item in list_items:
                if self._is_valid_product_element(item):
                    individual_products.append(str(item))
        
        # STRATEGY 2: Use data-liindex attribute
        if not individual_products and liindex_items:
            print("Using strategy 2: elements with data-liindex")
            for item in liindex_items:
                if self._is_valid_product_element(item):
                    individual_products.append(str(item))
        
        # STRATEGY 3: Use product thumbnail containers
        if not individual_products and thumbnail_containers:
            print("Using strategy 3: product thumbnail containers")
            for container in thumbnail_containers:
                if self._is_valid_product_element(container):
                    individual_products.append(str(container))
        
        # STRATEGY 4: Look for any container that has both product description and pricing
        if not individual_products:
            print("Using strategy 4: containers with product description and pricing")
            # Look for parent elements that contain both product description and pricing
            for desc in product_descriptions:
                parent = desc.find_parent(['li', 'div', 'article'])
                if parent and self._is_valid_product_element(parent):
                    individual_products.append(str(parent))
        
        # STRATEGY 5: Fallback - look for any element that contains product-like structure
        if not individual_products:
            print("Using strategy 5: aggressive search")
            # Look for elements that contain specific Macy's patterns
            potential_products = []
            
            # Look for elements containing specific Macy's classes
            macy_selectors = [
                '[class*="product-thumbnail"]',
                '[class*="product-description"]', 
                '[class*="pricing"]',
                '[data-v-0d91de5c]',  # Vue component
                '[data-v-b5fbd19c]'   # Vue component
            ]
            
            for selector in macy_selectors:
                elements = soup.select(selector)
                for elem in elements:
                    parent = elem.find_parent(['li', 'div'])
                    if parent and parent not in potential_products:
                        potential_products.append(parent)
            
            # Filter valid products
            for product in potential_products:
                if self._is_valid_product_element(product):
                    individual_products.append(str(product))
        
        # Remove duplicates while preserving order
        seen = set()
        unique_products = []
        for product in individual_products:
            if product not in seen:
                seen.add(product)
                unique_products.append(product)
        
        print(f"🎯 Final result: Found {len(unique_products)} unique product tiles in Macy's HTML")
        
        # DEBUG: Show what we found
        if unique_products:
            print("\n=== FOUND PRODUCTS PREVIEW ===")
            for i, product_html in enumerate(unique_products[:3]):  # Show first 3
                product_soup = BeautifulSoup(product_html, 'html.parser')
                name = self._extract_product_name(product_soup)
                price = self._extract_price(product_soup)
                print(f"Product {i+1}: Name='{name}', Price='{price}'")
        
        return unique_products

    def _is_valid_product_element(self, element) -> bool:
        """Check if an element is a valid product container - optimized for Macy's"""
        try:
            element_html = str(element)
            soup = BeautifulSoup(element_html, 'html.parser')
            
            # Check for Macy's specific indicators
            has_macys_image = bool(soup.find('img', src=re.compile(r'slimages\.macysassets\.com')))
            has_macys_price = bool(re.search(r'INR\s*[\d,]+\.?\d{2}', element_html))
            has_product_name = bool(soup.find(class_=re.compile(r'product-name|brand-and-name')))
            has_pricing = bool(soup.find(class_=re.compile(r'pricing|discount|price')))
            has_product_link = bool(soup.find('a', href=re.compile(r'/shop/product/')))
            
            # Check for specific Macy's Vue.js components
            has_vue_components = bool(re.search(r'data-v-[a-f0-9]+', element_html))
            
            # More specific criteria for Macy's
            indicators = [
                has_macys_image,
                has_macys_price, 
                has_product_name,
                has_pricing,
                has_product_link,
                has_vue_components
            ]
            
            score = sum(indicators)
            
            # Debug output for first few elements
            if len(str(element)) < 1000:  # Only debug smaller elements
                print(f"Validation: image={has_macys_image}, price={has_macys_price}, name={has_product_name}, pricing={has_pricing}, link={has_product_link}, vue={has_vue_components} = score:{score}")
            
            # Macy's products should have at least image + price + name, or similar combination
            return (has_macys_image and has_macys_price) or (has_product_name and has_pricing) or score >= 3
            
        except Exception as e:
            print(f"Error in _is_valid_product_element: {e}")
            return False
    def _extract_product_name(self, soup) -> str:
        """Extract product name from Macy's product tile"""
        # Try multiple selectors for product name
        name_selectors = [
            '.product-name',  # Product name class
            'h3.product-name',  # Product name header
            '.product-brand + h3',  # H3 after brand
            '.brand-and-name .product-name',  # Name in brand and name container
            '[data-v-b5fbd19c] h3',  # Vue data attribute
            '.product-description',  # Product description
            '[title]',  # Title attribute
            'a[href*="/shop/product/"]',  # Product links
            '.product-title',  # Product title
            '.item-name'  # Item name
        ]
        
        for selector in name_selectors:
            name_element = soup.select_one(selector)
            if name_element:
                name_text = name_element.get_text(strip=True)
                if name_text:
                    return self.clean_text(name_text)
        
        return "N/A"
    
    # def _extract_price(self, soup) -> str:
    #     """Extract price information from Macy's product - specialized version"""
    #     # Method 1: Try the discount price first (most common)
    #     discount_price = soup.select_one('.discount.is-tier2')
    #     if discount_price:
    #         # Get the first span inside the discount element which usually contains the price
    #         price_span = discount_price.select_one('span')
    #         if price_span:
    #             price_text = price_span.get_text(strip=True)
    #             extracted = self.extract_price_value(price_text)
    #             if extracted != "N/A":
    #                 return extracted
        
    #     # Method 2: Look for screen reader text which often contains the clean price
    #     screen_reader = soup.select_one('.show-for-sr')
    #     if screen_reader:
    #         sr_text = screen_reader.get_text()
    #         # Look for "Current price INR X,XXX.XX" pattern
    #         current_price_match = re.search(r'Current price\s+INR\s+([\d,]+(?:\.\d{2})?)', sr_text)
    #         if current_price_match:
    #             return f"INR {current_price_match.group(1)}"
        
    #     # Method 3: Look for any element with price-like content
    #     price_elements = soup.select('[class*="price"], [class*="Price"]')
    #     for element in price_elements:
    #         text = element.get_text(strip=True)
    #         extracted = self.extract_price_value(text)
    #         if extracted != "N/A":
    #             return extracted
        
    #     # Method 4: Fallback to text search
    #     return self.extract_price_value(soup.get_text())



    def _extract_price(self, soup) -> str:
        """Extract current, discount%, previous price from Macy's product"""

        current_price = None
        prev_price = None
        discount_percent = None

        # current price
        current_el = soup.select_one('.discount.is-tier2 span')
        if current_el:
            extracted = self.extract_price_value(current_el.get_text(strip=True))
            if extracted != "N/A":
                current_price = extracted

        # discount pct ex: "(75% off)"
        discount_el = soup.select_one('.discount.is-tier2 .sale-percent')
        if discount_el:
            pct = discount_el.get_text(strip=True)
            discount_percent = pct if pct else None

        # previous price
        prev_el = soup.select_one('.price-strike-sm')
        if prev_el:
            extracted = self.extract_price_value(prev_el.get_text(strip=True))
            if extracted != "N/A":
                prev_price = extracted

        # build output
        parts = []
        if current_price:
            parts.append(current_price)
        if discount_percent:
            parts.append(discount_percent)
        if prev_price:
            parts.append(prev_price)

        if parts:
            return " | ".join(parts)

        # fallback: screen-reader
        sr_el = soup.select_one('.show-for-sr')
        if sr_el:
            return self.extract_price_value(sr_el.get_text())

        # global fallback
        return self.extract_price_value(soup.get_text())

    

    def _extract_image(self, soup) -> str:
        """Extract product image URL from Macy's product"""
        # Image selectors
        img_selectors = [
            'li.slideshow-item.active img.picture-image',  # Active slideshow image
            'img.picture-image',  # Any picture image
            'img[data-src]',  # Lazy loaded images
            '.v-product-thumbnail-image-container img',  # Thumbnail container images
            '.product-image img',  # Product image
            '.v-slideshow img',  # Slideshow images
            'img[src*="slimages.macysassets.com"]',  # Macy's image CDN
            'img[loading="lazy"]',  # Lazy loaded images
            'img[itemprop="image"]',  # Schema image
            '.main-thumb img'  # Main thumbnail
        ]
        
        for selector in img_selectors:
            img_element = soup.select_one(selector)
            if img_element:
                # Try different attributes in priority order
                src_attrs = ['src', 'data-src', 'data-original', 'data-lazy']
                for attr in src_attrs:
                    img_url = img_element.get(attr)
                    if img_url and not img_url.startswith('data:'):
                        return self._normalize_image_url(img_url)
        
        return "N/A"
    
    def _extract_link(self, soup) -> str:
        """Extract product link from Macy's product"""
        # Link selectors
        link_selectors = [
            'a.brand-and-name',  # Brand and name link
            '.product-description a',  # Product description link
            '.product-thumbnail-container a',  # Thumbnail link
            'a[href*="/shop/product/"]',  # Product links
            '.description-spacing a',  # Description spacing link
            'a.product-link',  # Product link class
            'h2.name a',  # Name header link
            'a[itemprop="url"]'  # Schema URL
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
        """Extract badge information from Macy's product"""
        badges = []
        
        # Badge selectors
        badge_selectors = [
            '.badge-container',  # Badge container
            '.corner-badge',  # Corner badge
            '.badges-simplification',  # Badges simplification
            '.badge-wrapper',  # Badge wrapper
            '[class*="badge"]',  # Any badge class
            '.product-tag',  # Product tags
            '.promo-badge'  # Promotion badge
        ]
        
        for selector in badge_selectors:
            badge_elements = soup.select(selector)
            for badge in badge_elements:
                badge_text = self.clean_text(badge.get_text())
                if badge_text and badge_text not in badges:
                    badges.append(badge_text)
        
        return badges
    
    def _extract_promotions(self, soup) -> str:
        """Extract promotion text from Macy's product"""
        # Promotion selectors
        promo_selectors = [
            '.badges-simplification span',  # Badge text
            '.sale-percent',  # Sale percent
            '.bonus-offer',  # Bonus offer
            '.promotion-text',  # Promotion text
            '.discount-text',  # Discount text
            '.special-offer',  # Special offer
            '.deal-text'  # Deal text
        ]
        
        for selector in promo_selectors:
            promo_element = soup.select_one(selector)
            if promo_element:
                promo_text = self.clean_text(promo_element.get_text())
                if promo_text:
                    return promo_text
        
        return "N/A"
    
    def _normalize_image_url(self, url: str) -> str:
        """Normalize image URL for Macy's"""
        if not url:
            return "N/A"
        if url.startswith('http'):
            return url
        elif url.startswith('//'):
            return f"https:{url}"
        elif url.startswith('/'):
            return f"https://www.macys.com{url}"
        return url
    
    def _normalize_link_url(self, url: str) -> str:
        """Normalize link URL for Macy's"""
        if not url:
            return "N/A"
        if url.startswith('http'):
            return url
        elif url.startswith('//'):
            return f"https:{url}"
        elif url.startswith('/'):
            return f"https://www.macys.com{url}"
        return url

    def modify_image_url(self, image_url: str) -> str:
        """Modify the image URL to get higher resolution images for Macy's."""
        if not image_url or image_url == "N/A":
            return image_url

        # For Macy's images, modify to get higher resolution
        if 'slimages.macysassets.com' in image_url:
            # Replace width and height parameters for higher resolution
            modified_url = re.sub(r'wid=\d+', 'wid=1200', image_url)
            modified_url = re.sub(r'hei=\d+', 'hei=1200', modified_url)
            # Change quality if possible
            modified_url = re.sub(r'qlt=\d+', 'qlt=90', modified_url)
            return modified_url
        
        return image_url

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
        
        # Look for price patterns (Macy's uses INR)
        price_patterns = [
            r'INR\s*[\d,]+\.?\d*',  # INR price format
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
        
        # Diamond weight patterns
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
        
        # Gold type patterns for Macy's
        gold_patterns = [
            r'(\d{1,2}K)\s*(?:Yellow|White|Rose)\s*Gold',  # "14K Yellow Gold"
            r'(Yellow|White|Rose)\s*Gold\s*(\d{1,2}K)',  # "Yellow Gold 14K"
            r'(\d{1,2}K)\s*Gold',  # "14K Gold"
            r'(Platinum|Sterling Silver|Silver)',  # Other metals
            r'(Yellow Gold|White Gold|Rose Gold)',  # Gold colors
            r'(\d{1,2}K)\s*(?:YG|WG|RG)',  # "14K YG"
            r'(White|Yellow|Rose)\s*(\d{1,2}K)',  # "White 14K"
            r'in\s*(\d{1,2}K)\s*(?:White|Yellow|Rose)\s*Gold',  # "in 14K White Gold"
            r'(\d{1,2}K)\s*Gold\s*Over\s*Sterling\s*Silver',  # "14K Gold Over Sterling Silver"
            r'(\d{1,2}K)\s*Gold-Plated',  # "14K Gold-Plated"
            r'(\d{1,2}K)\s*Vermeil'  # "14K Vermeil"
        ]
        
        for pattern in gold_patterns:
            gold_match = re.search(pattern, text, re.IGNORECASE)
            if gold_match:
                # Return the matched groups, filtering out None
                gold_parts = [part for part in gold_match.groups() if part]
                return ' '.join(gold_parts).title()
        
        return "N/A"


