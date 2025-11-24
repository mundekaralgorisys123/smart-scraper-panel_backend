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
from PIL import Image
import io

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

IMAGE_SAVE_PATH = os.getenv("IMAGE_SAVE_PATH")
EXCEL_DATA_PATH = os.getenv("EXCEL_DATA_PATH")


class BulgariScraper:
    """Scraper for Bulgari product pages with database and Excel functionality"""
    
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
            print("=================== Starting Bulgari Scraper ==================")
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
            image_folder = os.path.join(self.image_save_path, f"bulgari_{timestamp}")
            os.makedirs(image_folder, exist_ok=True)
            
            # Create Excel file
            excel_filename = f"bulgari_scraped_products_{timestamp}.xlsx"
            excel_path = os.path.join(self.excel_data_path, excel_filename)
            
            # Process products
            database_records = []
            successful_downloads = 0
            
            # Create Excel workbook
            wb = Workbook()
            sheet = wb.active
            sheet.title = "Bulgari Products"
            
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
                    
                    # Download image - FIXED: Removed timestamp parameter
                    image_url = parsed_data.get('image_url')
                    image_path = self.download_image(
                        image_url, product_name, image_folder, unique_id  # Removed timestamp
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
                'website_type': 'bulgari',
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
        """Parse individual product HTML using Bulgari specific structure"""
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
        """Extract individual product HTML blocks from Bulgari HTML"""
        if not html_content:
            return []
        
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Bulgari specific product selectors
        product_selectors = [
            'div.product-tile',  # Main product tile
            '[data-testid="product-tile"]',  # Product tile with testid
            '.chakra-aspect-ratio.product-tile',  # Product tile with classes
        ]
        
        individual_products = []
        
        for selector in product_selectors:
            product_tiles = soup.select(selector)
            for tile in product_tiles:
                # Skip editorial tiles or non-product items
                if not tile.select('.product-tile__title'):
                    continue
                individual_products.append(str(tile))
            if individual_products:
                break
        
        print(f"Found {len(individual_products)} product tiles in Bulgari HTML")
        return individual_products
    
    def _extract_product_name(self, soup) -> str:
        """Extract product name from Bulgari product tile including gold type"""
        name_selectors = [
            '.product-tile__title',  # Product title
            'h2.chakra-heading',  # Heading
            '[data-testid="product-name"]',  # Data attribute
        ]
        
        for selector in name_selectors:
            name_element = soup.select_one(selector)
            if name_element:
                product_name = self.clean_text(name_element.get_text())
                
                # Also look for gold type/material information
                gold_type = self._extract_gold_type_from_product_tile(soup)
                if gold_type and gold_type != "N/A":
                    return f"{product_name} {gold_type}"
                
                return product_name
        
        return "N/A"

    def _extract_gold_type_from_product_tile(self, soup) -> str:
        """Extract gold type specifically from product tile structure"""
        # Look for material/gold type in the product tile
        material_selectors = [
            '.chakra-text.css-16yz1ii',  # Material text (like "Yellow gold")
            '.product-tile__material',
            '.product-tile__details p',
            '[data-testid="product-material"]',
        ]
        
        for selector in material_selectors:
            material_element = soup.select_one(selector)
            if material_element:
                material_text = self.clean_text(material_element.get_text())
                # Check if it's actually a gold type
                gold_type = self.extract_gold_type_value(material_text)
                if gold_type != "N/A":
                    return gold_type
                # If not a recognized gold type but contains gold/platinum, return as is
                if any(word in material_text.lower() for word in ['gold', 'platinum', 'silver', 'rose', 'white', 'yellow']):
                    return material_text
        
        return "N/A"
    
    def _extract_price(self, soup) -> str:
        """Extract price from Bulgari product"""
        price_selectors = [
            '[data-testid="product-tile-price"]',  # Price with testid
            '.product-tile__price',  # Price class
            '.chakra-text.product-tile__price',  # Price text
        ]
        
        for selector in price_selectors:
            price_element = soup.select_one(selector)
            if price_element:
                price_text = price_element.get_text(strip=True)
                # Clean up price string
                price_text = re.sub(r'\s+', ' ', price_text).strip()
                return self.extract_price_value(price_text)
        
        return "N/A"
    
    def _extract_image(self, soup) -> str:
        """Extract image URL from Bulgari product"""
        # Bulgari specific image selectors - get the first/main image
        img_selectors = [
            '.product-tile__image',  # Product tile image
            'img[data-testid="cloudinary-img-srcset"]',  # Cloudinary images
            '.product-tile__image-wrapper--main img',  # Main image
            '.snap-slider__slide--active img',  # Active slide image
        ]
        
        for selector in img_selectors:
            img_element = soup.select_one(selector)
            if img_element:
                image_url = img_element.get('src')
                if image_url:
                    return self._normalize_bulgari_image_url(image_url)
        
        return "N/A"
    
    def _normalize_bulgari_image_url(self, url: str) -> str:
        """Normalize image URL for Bulgari - enhance quality to maximum"""
        if not url or url == "N/A":
            return "N/A"
        
        # Ensure URL is complete
        if url.startswith('//'):
            url = f"https:{url}"
        
        # Enhance image quality to maximum resolution
        if 'media.bulgari.com' in url:
            # Remove ALL size restrictions for maximum quality
            url = re.sub(r'/c_[^/]+/', '/c_limit,f_auto/', url)  # Remove crop parameters
            url = re.sub(r'/h_\d+,w_\d+/', '/h_2000,w_2000/', url)  # Set to max dimensions
            url = re.sub(r'\?.*$', '?q_auto:best', url)  # Ensure best quality
            
            # Specific replacements for common patterns
            url = url.replace("h_490,w_490", "h_2000,w_2000")
            url = url.replace("h_670,w_490", "h_2000,w_2000") 
            url = url.replace("c_pad", "c_limit")  # Change from pad to limit for better quality
            url = url.replace("q_auto", "q_auto:best")  # Ensure best quality
            
            # If no quality parameter, add it
            if 'q_auto' not in url and 'q_' not in url:
                if '?' in url:
                    url += '&q_auto:best'
                else:
                    url += '?q_auto:best'
        
        return url
    
    def _extract_link(self, soup) -> str:
        """Extract product link from Bulgari product"""
        link_selectors = [
            '.product-tile__anchor',  # Product anchor
            'a[href*="/en-us/"]',  # Links with en-us path
            'a.chakra-link',  # Chakra link
        ]
        
        for selector in link_selectors:
            link_element = soup.select_one(selector)
            if link_element and link_element.get('href'):
                href = link_element.get('href')
                return self._normalize_link_url(href)
        
        return "N/A"
    
    def _normalize_link_url(self, url: str) -> str:
        """Normalize link URL for Bulgari"""
        if not url or url == "N/A":
            return "N/A"
        if url.startswith('http'):
            return url
        elif url.startswith('//'):
            return f"https:{url}"
        elif url.startswith('/'):
            return f"https://www.bulgari.com{url}"
        return url
    
    def _extract_diamond_weight(self, soup) -> str:
        """Extract diamond weight from product name and description"""
        product_name = self._extract_product_name(soup)
        description = self._extract_description(soup)
        
        # Try product name first
        diamond_weight = self.extract_diamond_weight_value(product_name)
        if diamond_weight != "N/A":
            return diamond_weight
        
        # Then try description
        return self.extract_diamond_weight_value(description)
    
    def _extract_description(self, soup) -> str:
        """Extract product description"""
        # For Bulgari, description often includes material info
        material_selectors = [
            '.chakra-text.css-16yz1ii',  # Material text
            '.product-tile__details p',  # Details paragraph
        ]
        
        for selector in material_selectors:
            material_element = soup.select_one(selector)
            if material_element:
                return self.clean_text(material_element.get_text())
        
        return "N/A"
    
    def _extract_gold_type(self, soup) -> str:
        """Extract gold type from product description"""
        description = self._extract_description(soup)
        gold_type = self.extract_gold_type_value(description)
        
        if gold_type != "N/A":
            return gold_type
        
        # Also check product name
        product_name = self._extract_product_name(soup)
        return self.extract_gold_type_value(product_name)
    
    def _extract_badges(self, soup) -> list:
        """Extract badges and additional info from Bulgari product"""
        badges = []
        
        # Extract flags/tags (e.g., "Customize It")
        flag_selectors = [
            '.product-tile__flag',  # Product flag
            'p.product-tile__flag',  # Flag paragraph
        ]
        
        for selector in flag_selectors:
            flag_elements = soup.select(selector)
            for flag in flag_elements:
                flag_text = self.clean_text(flag.get_text())
                if flag_text and flag_text not in badges:
                    badges.append(flag_text)
        
        # Extract material information
        material = self._extract_description(soup)
        if material and material != "N/A" and material not in badges:
            badges.append(material)
        
        return badges
    
    def _extract_promotions(self, soup) -> str:
        """Extract promotion text from Bulgari product"""
        # Look for current price mentions
        current_price_selectors = [
            '.css-idkz9h',  # Current price text
            '[aria-label*="Current price"]',  # Aria label with current price
        ]
        
        promotions = []
        for selector in current_price_selectors:
            price_elements = soup.select(selector)
            for price_el in price_elements:
                price_text = self.clean_text(price_el.get_text())
                if "current price" in price_text.lower() and price_text not in promotions:
                    promotions.append(price_text)
        
        return " | ".join(promotions) if promotions else "N/A"
    


    # def download_image(self, image_url: str, product_name: str, image_folder: str, unique_id: str, retries: int = 2) -> str:
    #     """
    #     Download images for Bulgari without enhancement or conversion
    #     """
    #     print("=================== Downloading Image ==================")
    #     print(f"Downloading image for product: {image_url}")
    #     print("=================== Downloading Image ==================")
    #     if not image_url or image_url == "N/A":
    #         return "N/A"

    #     # Clean filename
    #     clean_name = self._clean_filename(product_name)
    #     if len(clean_name) > 50:
    #         clean_name = clean_name[:50]

    #     logger.info(f"Downloading image for: {product_name}")
    #     logger.info(f"Original URL: {image_url}")

    #     for attempt in range(retries):
    #         try:
    #             logger.info(f"Attempt {attempt + 1} with URL: {image_url}")
                
    #             headers = {
    #                 "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    #                 "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
    #                 "Accept-Language": "en-US,en;q=0.9",
    #                 "Referer": "https://www.bulgari.com/",
    #             }

    #             response = requests.get(image_url, headers=headers, timeout=15, stream=True)
                
    #             logger.info(f"Response status: {response.status_code}")
                
    #             if response.status_code == 200:
    #                 content_type = response.headers.get('content-type', '')
    #                 logger.info(f"Content-Type: {content_type}")
                    
    #                 if not content_type.startswith('image/'):
    #                     logger.warning(f"Non-image content type: {content_type}")
    #                     continue

    #                 # Get file extension from content type
    #                 file_extension = self._get_file_extension_from_content_type(content_type)
    #                 image_filename = f"{unique_id}_{clean_name}{file_extension}"
    #                 image_full_path = os.path.join(image_folder, image_filename)

    #                 # Save image as-is without conversion
    #                 with open(image_full_path, "wb") as f:
    #                     for chunk in response.iter_content(chunk_size=8192):
    #                         if chunk:
    #                             f.write(chunk)

    #                 # Verify download
    #                 if os.path.exists(image_full_path):
    #                     file_size = os.path.getsize(image_full_path)
    #                     if file_size > 1000:
    #                         logger.info(f"✅ Successfully downloaded: {product_name} ({file_size} bytes) as {file_extension}")
    #                         return image_full_path
    #                     else:
    #                         logger.warning(f"Downloaded file too small: {file_size} bytes")
    #                         os.remove(image_full_path)
    #                 else:
    #                     logger.warning("File was not created")
                        
    #             elif response.status_code == 404:
    #                 logger.warning(f"Image not found (404): {image_url}")
    #                 break  # Don't retry if 404
                    
    #         except requests.exceptions.Timeout:
    #             logger.warning(f"Timeout on attempt {attempt + 1}")
    #         except requests.exceptions.ConnectionError as e:
    #             logger.warning(f"Connection error on attempt {attempt + 1}: {str(e)}")
    #         except requests.exceptions.RequestException as e:
    #             logger.warning(f"Request exception on attempt {attempt + 1}: {e}")
    #         except Exception as e:
    #             logger.warning(f"Unexpected error on attempt {attempt + 1}: {e}")
            
    #         # Wait before retry
    #         if attempt < retries - 1:
    #             time.sleep(1)

    #     logger.error(f"❌ Failed to download: {product_name}")
    #     return "N/A"


    def convert_bulgari_url_to_jpg(self, url: str) -> str:
        # Replace first f_auto → f_jpg
        url = url.replace("f_auto", "f_jpg", 1)

        # Remove second f_auto/ if exists
        url = url.replace("f_auto/", "")

        # Force JPG extension
        url = re.sub(r"\.(png|jpg|jpeg|avif|webp)$", ".jpg", url)

        return url



    def download_image(self, image_url: str, product_name: str, image_folder: str, unique_id: str, retries: int = 2) -> str:
        """
        Download images for Bulgari (always JPG). No AVIF conversion needed.
        """
        print("=================== Downloading Image ==================")
        print(f"Downloading image for product: {image_url}")
        print("========================================================")

        if not image_url or image_url == "N/A":
            return "N/A"

        # Clean filename
        clean_name = self._clean_filename(product_name)
        if len(clean_name) > 50:
            clean_name = clean_name[:50]

        logger.info(f"Downloading image for: {product_name}")
        logger.info(f"Original URL: {image_url}")

        # 🔥 FORCE JPG URL
        image_url = self.convert_bulgari_url_to_jpg(image_url)

        for attempt in range(retries):
            try:
                logger.info(f"Attempt {attempt + 1} with URL: {image_url}")

                headers = {
                    "User-Agent": "Mozilla/5.0",
                    "Accept": "image/jpg,image/jpeg,image/*;q=0.8,*/*;q=0.5",
                    "Referer": "https://www.bulgari.com/",
                }

                response = requests.get(image_url, headers=headers, timeout=15, stream=True)
                logger.info(f"Response status: {response.status_code}")

                if response.status_code != 200:
                    continue

                # Always save as JPG
                final_filename = f"{unique_id}_{clean_name}.jpg"
                final_full_path = os.path.join(image_folder, final_filename)

                with open(final_full_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)

                # Validate image download
                if os.path.exists(final_full_path) and os.path.getsize(final_full_path) > 1000:
                    logger.info(f"✅ Successfully downloaded JPG: {final_full_path}")
                    return final_full_path
                else:
                    logger.warning("Downloaded file too small, retrying...")
                    continue

            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed: {e}")

            time.sleep(1)

        logger.error(f"❌ Failed to download: {product_name}")
        return "N/A"


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
        
        # Bulgari price formats: "$3,200.00", "$16,600.00"
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
            r'(Rose\s*Gold|White\s*Gold|Yellow\s*Gold)',
            r'(\d+k)\s*(?:White|Yellow|Rose)?\s*Gold',
            r'(Platinum|Gold)',
        ]
        
        for pattern in gold_patterns:
            gold_match = re.search(pattern, text, re.IGNORECASE)
            if gold_match:
                gold_type = gold_match.group(1)
                return gold_type.title()
        
        return "N/A"