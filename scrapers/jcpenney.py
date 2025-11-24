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


class JCPenneyParser:
    """Parser for JCPenney product pages with database and Excel functionality"""
    
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
            print("=================== Starting JCPenney Parser ==================")
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
            image_folder = os.path.join(self.image_save_path, f"jcpenney_{timestamp}")
            os.makedirs(image_folder, exist_ok=True)
            
            # Create Excel file
            excel_filename = f"jcpenney_scraped_products_{timestamp}.xlsx"
            excel_path = os.path.join(self.excel_data_path, excel_filename)
            
            # Process products
            database_records = []
            successful_downloads = 0
            
            # Create Excel workbook
            wb = Workbook()
            sheet = wb.active
            sheet.title = "JCPenney Products"
            
            # Add headers
            headers = [
                'Unique ID', 'Current Date', 'Page Title', 'Product Name', 
                'Image Path', 'Gold Type', 'Price', 'Diamond Weight', 
                'Additional Info', 'Scrape Time', 'Image URL', 'Product Link',
                'Session ID', 'Page URL', 'Original Price', 'Discount Code',
                'Rating', 'Colors', 'Promotion Text'
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
                    
                    # Prepare additional info with all the new fields
                    additional_info = self._build_additional_info(parsed_data)
                    
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
                        page_url,
                        parsed_data.get('original_price', 'N/A'),
                        parsed_data.get('discount_code', 'N/A'),
                        parsed_data.get('rating', 'N/A'),
                        parsed_data.get('colors', 'N/A'),
                        parsed_data.get('promotion_text', 'N/A')
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
                'website_type': 'jcpenney',
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
            'original_price': self._extract_original_price(soup),
            'image_url': self._extract_image(soup),
            'link': self._extract_link(soup),
            'diamond_weight': self._extract_diamond_weight(soup),
            'gold_type': self._extract_gold_type(soup),
            'badges': self._extract_badges(soup),
            'promotions': self._extract_promotions(soup),
            'discount_code': self._extract_discount_code(soup),
            'rating': self._extract_rating(soup),
            'colors': self._extract_colors(soup),
            'promotion_text': self._extract_promotion_text(soup)
        }
    
    def _build_additional_info(self, parsed_data: Dict[str, Any]) -> str:
        """Build additional info string from all available product data"""
        additional_info_parts = []
        
        # Add badges
        badges = parsed_data.get('badges', [])
        if badges:
            additional_info_parts.extend(badges)
        
        # Add promotions
        promotions = parsed_data.get('promotions', '')
        if promotions and promotions != "N/A":
            additional_info_parts.append(promotions)
        
        # Add rating
        rating = parsed_data.get('rating', '')
        if rating and rating != "N/A":
            additional_info_parts.append(f"Rating: {rating}")
        
        # Add colors
        colors = parsed_data.get('colors', '')
        if colors and colors != "N/A":
            additional_info_parts.append(f"Colors: {colors}")
        
        # Add discount code
        discount_code = parsed_data.get('discount_code', '')
        if discount_code and discount_code != "N/A":
            additional_info_parts.append(f"Coupon: {discount_code}")
        
        # Add promotion text
        promotion_text = parsed_data.get('promotion_text', '')
        if promotion_text and promotion_text != "N/A":
            additional_info_parts.append(promotion_text)
        
        return " | ".join(additional_info_parts) if additional_info_parts else "N/A"
    
    def extract_individual_products_from_html(self, html_content: str) -> List[str]:
        """Extract individual product HTML blocks from JCPenney HTML"""
        if not html_content:
            return []
        
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # JCPenney specific product selectors
        product_selectors = [
            'li[data-automation-id^="list-item-"]',  # List items with automation IDs
            '.ProductCard-productCardPane',  # Product card container
            '.Rkqsa.G85iV.yJy1z',  # Product wrapper
            'li.pAB7b.D2LxB.gQ4Qt',  # Product list item
            '[data-ppid]',  # Elements with product ID
            '.YOMPJ.KUgp0.tJYbI'  # Product card
        ]
        
        individual_products = []
        
        for selector in product_selectors:
            product_tiles = soup.select(selector)
            for tile in product_tiles:
                individual_products.append(str(tile))
            if individual_products:
                break  # Stop if we found products with this selector
        
        print(f"Found {len(individual_products)} product tiles in JCPenney HTML")
        return individual_products
    
    def _extract_product_name(self, soup) -> str:
        """Extract product name from JCPenney product tile"""
        # JCPenney specific name selectors
        name_selectors = [
            'a[data-automation-id="product-title"]',  # Product title with automation ID
            '.-zrMP.FMQQD.t689a',  # Product title class
            '.product-title a',  # Alternative product title
            'h2 a',  # Header link
            '[title] a'  # Any link with title
        ]
        
        for selector in name_selectors:
            name_element = soup.select_one(selector)
            if name_element:
                name_text = name_element.get_text(strip=True)
                if name_text:
                    return self.clean_text(name_text)
        
        # Fallback: look for any anchor with substantial text
        anchors = soup.find_all('a', string=re.compile(r'.{10,}'))
        for anchor in anchors:
            text = anchor.get_text(strip=True)
            if len(text) > 10 and not any(word in text.lower() for word in ['sort', 'filter', 'page']):
                return self.clean_text(text)
        
        return "N/A"
    
    # def _extract_price(self, soup) -> str:
    #     """Extract current price from JCPenney product"""
    #     # Current price selectors for JCPenney
    #     price_selectors = [
    #         '.DXCCO._2Bk5a.wrap',  # Current price span
    #         '.sales-price .price',  # Sales price
    #         '[data-automation-id="product-price"] .price',  # Price in product price container
    #         '.current-price',  # Current price
    #         '.gallery .price'  # Gallery price
    #     ]
        
    #     for selector in price_selectors:
    #         price_element = soup.select_one(selector)
    #         if price_element:
    #             price_text = price_element.get_text(strip=True)
    #             extracted_price = self.extract_price_value(price_text)
    #             if extracted_price != "N/A":
    #                 return extracted_price
        
    #     # Look for price patterns in the entire product HTML
    #     price_match = re.search(r'\$\d{1,3}(?:,\d{3})*(?:\.\d{2})?', soup.get_text())
    #     if price_match:
    #         return price_match.group(0)
        
    #     return "N/A"


    def _extract_price(self, soup) -> str:
        """Extract current + previous price from JCPenney product"""

        current_selectors = [
            '[data-automation-id="at-price-value"]',        # current span
            '.sales-price .price',
            '.current-price',
            '[data-automation-id="product-price"] .price',
            '.DXCCO._2Bk5a.wrap',
            '.gallery .price'
        ]

        previous_selectors = [
            '[data-automation-id="price-old-sale"] strike',
            '.old-price strike',
            'strike',
        ]

        current_price = None
        previous_price = None

        # Extract current price
        for selector in current_selectors:
            price_element = soup.select_one(selector)
            if price_element:
                text = price_element.get_text(strip=True)
                val = self.extract_price_value(text)
                if val != "N/A":
                    current_price = val
                    break

        # Extract previous price
        for selector in previous_selectors:
            price_element = soup.select_one(selector)
            if price_element:
                text = price_element.get_text(strip=True)
                val = self.extract_price_value(text)
                if val != "N/A":
                    previous_price = val
                    break

        if current_price and previous_price:
            return f"{current_price} | {previous_price}"

        if current_price:
            return current_price

        if previous_price:
            return previous_price

        # last fallback, regex scan
        price_match = re.search(r'\$\d{1,3}(?:,\d{3})*(?:\.\d{2})?', soup.get_text())
        if price_match:
            return price_match.group(0)
        
        return "N/A"

    
    def _extract_original_price(self, soup) -> str:
        """Extract original price (strikethrough) from JCPenney product"""
        # Original price selectors
        original_price_selectors = [
            'strike',  # Strikethrough price
            '.original-price',  # Original price class
            '.price-old-sale',  # Old sale price
            '._8uOFg strike'  # Strike in price container
        ]
        
        for selector in original_price_selectors:
            price_element = soup.select_one(selector)
            if price_element:
                price_text = price_element.get_text(strip=True)
                extracted_price = self.extract_price_value(price_text)
                if extracted_price != "N/A":
                    return extracted_price
        
        return "N/A"
    
    def _extract_image(self, soup) -> str:
        """Extract product image URL from JCPenney product"""
        # JCPenney image selectors
        img_selectors = [
            'img[loading="lazy"]',  # Lazy loaded images
            'img.visible.KVxnG',  # Visible product image
            '.product-image img',  # Product image
            'img[src*="scene7.com"]',  # JCPenney scene7 images
            'img[alt*="product"]',  # Product images with alt text
            '.main-image img'  # Main image
        ]
        
        for selector in img_selectors:
            img_element = soup.select_one(selector)
            if img_element and img_element.get('src'):
                src = img_element.get('src')
                return self._normalize_image_url(src)
        
        return "N/A"
    
    def _extract_link(self, soup) -> str:
        """Extract product link from JCPenney product"""
        # Link selectors for JCPenney
        link_selectors = [
            'a[data-automation-id="product-title"]',  # Product title link
            'a.mChv9.KUgp0',  # Product card link
            'a[href*="/p/"]',  # Links with product path
            '.product-link a',  # Product link
            'a[title]'  # Links with title
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
        """Extract badge information from JCPenney product"""
        badges = []
        
        # JCPenney badge selectors
        badge_selectors = [
            '.product-badge',  # Product badges
            '.badge-container',  # Badge container
            '.promo-badge',  # Promotion badge
            '.sale-badge',  # Sale badge
            '[class*="badge"]'  # Any badge class
        ]
        
        for selector in badge_selectors:
            badge_elements = soup.select(selector)
            for badge in badge_elements:
                badge_text = self.clean_text(badge.get_text())
                if badge_text and badge_text not in badges:
                    badges.append(badge_text)
        
        return badges
    
    def _extract_promotions(self, soup) -> str:
        """Extract promotion text from JCPenney product"""
        # Promotion text selectors
        promo_selectors = [
            '.H-M5g.yxA5D.newFPACCouponText',  # "with code" text
            '.promo-text',  # Promotion text
            '.discount-text',  # Discount text
            '.sale-text',  # Sale text
            '._8uOFg'
        ]
        
        for selector in promo_selectors:
            promo_element = soup.select_one(selector)
            if promo_element:
                promo_text = promo_element.get_text(strip=True)
                if promo_text:
                    return promo_text
        
        return "N/A"
    
    def _extract_discount_code(self, soup) -> str:
        """Extract discount code from JCPenney product"""

        # Case 1: Standard fpacCoupon input
        discount_input = soup.find('input', class_='fpacCoupon')
        if discount_input and discount_input.get('value'):
            return discount_input['value'].strip()

        # Case 2: Discount code shown in text like: "Use code SAVE20"
        possible_text_selectors = [
            '.H-M5g.yxA5D.newFPACCouponText',   # JCP code block
            '.promo-text',
            '.discount-code',
            '.coupon-text'
        ]

        for selector in possible_text_selectors:
            el = soup.select_one(selector)
            if el:
                text = el.get_text(" ", strip=True)
                # Look for patterns like SAVE20, EXTRA15, etc.
                match = re.search(r'\b[A-Z0-9]{4,10}\b', text)
                if match:
                    return match.group(0)

        return "N/A"

    
    def _extract_rating(self, soup) -> str:
        """Extract product rating from JCPenney product"""
        # Rating selectors
        rating_selectors = [
            'div[data-automation-id="productCard-automation-rating"]',  # Rating container
            '.Meqrf.PI6hD.SZ2pn',  # Rating count
            '.product-rating',  # Product rating
            '.rating-count'  # Rating count
        ]
        
        for selector in rating_selectors:
            rating_element = soup.select_one(selector)
            if rating_element:
                rating_text = self.clean_text(rating_element.get_text())
                if rating_text:
                    # Clean up rating text - remove extra whitespace
                    cleaned_rating = " ".join(rating_text.split())
                    return cleaned_rating
        
        return "N/A"
    
    def _extract_colors(self, soup) -> str:
        """Extract color options from JCPenney product"""
        try:
            # Look for color selection buttons with images
            color_buttons = soup.select('button.qMneo img')
            if color_buttons:
                colors = []
                for btn in color_buttons:
                    alt_text = btn.get('alt', '')
                    if alt_text and alt_text.lower() != 'null':
                        colors.append(alt_text)
                if colors:
                    return ', '.join(colors)
        except Exception as e:
            logger.debug(f"Error extracting colors: {e}")
        
        return "N/A"
    
    def _extract_promotion_text(self, soup) -> str:
        """Extract additional promotion text from JCPenney product"""
        # Look for various promotion text elements
        promo_text_selectors = [
            '.promotion-message',
            '.deal-text',
            '.special-offer',
            '.savings-text'
        ]
        
        for selector in promo_text_selectors:
            promo_element = soup.select_one(selector)
            if promo_element:
                promo_text = self.clean_text(promo_element.get_text())
                if promo_text:
                    return promo_text
        
        return "N/A"
    
    def _normalize_image_url(self, url: str) -> str:
        """Normalize image URL for JCPenney"""
        if not url:
            return "N/A"
        if url.startswith('http'):
            return url
        elif url.startswith('//'):
            return f"https:{url}"
        elif url.startswith('/'):
            return f"https://www.jcpenney.com{url}"
        return url
    
    def _normalize_link_url(self, url: str) -> str:
        """Normalize link URL for JCPenney"""
        if not url:
            return "N/A"
        if url.startswith('http'):
            return url
        elif url.startswith('//'):
            return f"https:{url}"
        elif url.startswith('/'):
            return f"https://www.jcpenney.com{url}"
        return url

    def modify_image_url(self, image_url: str) -> str:
        """Modify the image URL to get higher resolution images for JCPenney."""
        if not image_url or image_url == "N/A":
            return image_url

        # For JCPenney scene7 images, modify to get higher resolution
        if 'scene7.com' in image_url:
            # Replace width and height parameters for higher resolution
            modified_url = re.sub(r'wid=\d+', 'wid=800', image_url)
            modified_url = re.sub(r'hei=\d+', 'hei=800', modified_url)
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
        
        # Gold type patterns for JCPenney
        gold_patterns = [
            r'(\d{1,2}K)\s*(?:Yellow|White|Rose)\s*Gold',  # "14K Yellow Gold"
            r'(Yellow|White|Rose)\s*Gold\s*(\d{1,2}K)',  # "Yellow Gold 14K"
            r'(\d{1,2}K)\s*Gold',  # "14K Gold"
            r'(Platinum|Sterling Silver|Silver)',  # Other metals
            r'(Yellow Gold|White Gold|Rose Gold)',  # Gold colors
            r'(\d{1,2}K)\s*(?:YG|WG|RG)',  # "14K YG"
            r'(White|Yellow|Rose)\s*(\d{1,2}K)',  # "White 14K"
            r'in\s*(\d{1,2}K)\s*(?:White|Yellow|Rose)\s*Gold',  # "in 14K White Gold"
            r'(\d{1,2}K)\s*Gold\s*Over\s*Brass',  # "14K Gold Over Brass"
            r'Pure\s*Silver\s*Over\s*Brass'  # "Pure Silver Over Brass"
        ]
        
        for pattern in gold_patterns:
            gold_match = re.search(pattern, text, re.IGNORECASE)
            if gold_match:
                # Return the matched groups, filtering out None
                gold_parts = [part for part in gold_match.groups() if part]
                return ' '.join(gold_parts).title()
        
        return "N/A"