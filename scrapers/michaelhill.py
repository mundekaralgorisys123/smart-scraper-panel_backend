# import base64
# import os
# import uuid
# from datetime import datetime
# from bs4 import BeautifulSoup
# import re
# from typing import Dict, Any, List
# import requests
# from urllib.parse import urlparse
# from openpyxl import Workbook
# from database.db_inseartin import insert_into_db, update_product_count


# BASE_DIR = os.path.abspath(os.path.dirname(__file__))
# EXCEL_DATA_PATH = os.path.join(BASE_DIR, 'static', 'ExcelData')
# IMAGE_SAVE_PATH = os.path.join(BASE_DIR, 'static', 'Images')


# # Ensure directories exist
# os.makedirs(EXCEL_DATA_PATH, exist_ok=True)
# os.makedirs(IMAGE_SAVE_PATH, exist_ok=True)


# class MichaelHillParser:
#     """Parser for Michael Hill product pages with database and Excel functionality"""
    
#     def __init__(self, excel_data_path='static/ExcelData', image_save_path='static/Images'):
#         self.excel_data_path = excel_data_path
#         self.image_save_path = image_save_path
#         self.setup_directories()
    
#     def setup_directories(self):
#         """Create necessary directories"""
#         os.makedirs(self.excel_data_path, exist_ok=True)
#         os.makedirs(self.image_save_path, exist_ok=True)
    
#     def parse_and_save_products(self, products_data: List[Dict], page_title: str, page_url: str = "") -> Dict[str, Any]:
#         """
#         Main method to parse products and save to database/Excel
#         Returns: JSON response compatible with your requirements
#         """
#         try:
#             print("=================== Starting Michael Hill Parser ==================")
#             print(f"Processing {len(products_data)} product entries")
            
#             # Extract HTML content
#             html_content = products_data[0].get('html', '') if products_data else ''
            
#             # Parse individual products from HTML
#             individual_products = self.extract_individual_products_from_html(html_content)
#             print(f"Extracted {len(individual_products)} individual products")
            
#             # Generate unique session ID and timestamp
#             session_id = str(uuid.uuid4())
#             timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
#             current_date = datetime.now().date()
#             current_time = datetime.now().time()
            
#             # Create image folder for this session
#             image_folder = os.path.join(self.image_save_path, f"michaelhill_{timestamp}")
#             os.makedirs(image_folder, exist_ok=True)
            
#             # Create Excel file
#             excel_filename = f"michaelhill_scraped_products_{timestamp}.xlsx"
#             excel_path = os.path.join(self.excel_data_path, excel_filename)
            
#             # Process products
#             database_records = []
#             successful_downloads = 0
            
#             # Create Excel workbook
#             wb = Workbook()
#             sheet = wb.active
#             sheet.title = "Michael Hill Products"
            
#             # Add headers
#             headers = [
#                 'Unique ID', 'Current Date', 'Page Title', 'Product Name', 
#                 'Image Path', 'Gold Type', 'Price', 'Diamond Weight', 
#                 'Additional Info', 'Scrape Time', 'Image URL', 'Product Link',
#                 'Session ID', 'Page URL'
#             ]
#             sheet.append(headers)
            
#             # Process each product
#             for i, product_html in enumerate(individual_products):
#                 try:
#                     # Parse product data
#                     parsed_data = self.parse_product(product_html)
                    
#                     # Generate unique ID
#                     unique_id = str(uuid.uuid4())
#                     product_name = parsed_data.get('product_name', 'Unknown Product')[:495]
                    
#                     # Download image
#                     image_url = parsed_data.get('image_url')
#                     image_path = self.download_image_sync(
#                         image_url, product_name, timestamp, image_folder, unique_id
#                     )
                    
#                     if image_path != "N/A":
#                         successful_downloads += 1
                    
#                     # Prepare additional info
#                     badges = parsed_data.get('badges', [])
#                     promotions = parsed_data.get('promotions', '')
#                     additional_info_parts = []
                    
#                     if badges:
#                         additional_info_parts.extend(badges)
#                     if promotions and promotions != "N/A":
#                         additional_info_parts.append(promotions)
                    
#                     additional_info = " | ".join(additional_info_parts) if additional_info_parts else "N/A"
                    
#                     # Create database record
#                     db_record = {
#                         'unique_id': unique_id,
#                         'current_date': current_date,
#                         'page_title': page_title,
#                         'product_name': product_name,
#                         'image_path': image_path,
#                         'price': parsed_data.get('price'),
#                         'diamond_weight': parsed_data.get('diamond_weight'),
#                         'gold_type': parsed_data.get('gold_type'),
#                         'additional_info': additional_info,
#                     }
                    
#                     database_records.append(db_record)
                    
#                     # Add to Excel
#                     sheet.append([
#                         unique_id,
#                         current_date.strftime('%Y-%m-%d'),
#                         page_title,
#                         product_name,
#                         image_path,
#                         parsed_data.get('gold_type', 'N/A'),
#                         parsed_data.get('price', 'N/A'),
#                         parsed_data.get('diamond_weight', 'N/A'),
#                         additional_info,
#                         current_time.strftime('%H:%M:%S'),
#                         image_url,
#                         parsed_data.get('link', 'N/A'),
#                         session_id,
#                         page_url
#                     ])
                    
#                     print(f"Processed product {i+1}: {product_name}")
                    
#                 except Exception as e:
#                     print(f"Error processing product {i}: {e}")
#                     continue
            
#             # Save Excel file
#             wb.save(excel_path)
#             print(f"Excel file saved: {excel_path}")
            
#             # Insert data into the database and update product count
#             insert_into_db(database_records)
#             update_product_count(len(database_records))
            
#             # Encode Excel file to base64
#             with open(excel_path, "rb") as file:
#                 base64_file = base64.b64encode(file.read()).decode("utf-8")
            
#             # Return JSON response
#             return {
#                 'message': f'Successfully processed {len(database_records)} products',
#                 'session_id': session_id,
#                 'excel_file': excel_filename,
#                 'total_processed': len(database_records),
#                 'images_downloaded': successful_downloads,
#                 'failed': len(individual_products) - len(database_records),
#                 'website_type': 'michaelhill',
#                 'base64_file': base64_file,
#                 'file_path': excel_path
#             }
            
#         except Exception as e:
#             print(f"Error in parse_and_save_products: {e}")
#             return {
#                 'error': str(e),
#                 'message': 'Failed to process products'
#             }
    
#     def parse_product(self, product_html: str) -> Dict[str, Any]:
#         """Parse individual product HTML"""
#         soup = BeautifulSoup(product_html, 'html.parser')
        
#         return {
#             'product_name': self._extract_product_name(soup),
#             'price': self._extract_price(soup),
#             'image_url': self._extract_image(soup),
#             'link': self._extract_link(soup),
#             'diamond_weight': self._extract_diamond_weight(soup),
#             'gold_type': self._extract_gold_type(soup),
#             'badges': self._extract_badges(soup),
#             'promotions': self._extract_promotions(soup)
#         }
    
#     def extract_individual_products_from_html(self, html_content: str) -> List[str]:
#         """Extract individual product HTML blocks"""
#         if not html_content:
#             return []
        
#         soup = BeautifulSoup(html_content, 'html.parser')
#         product_tiles = soup.find_all('div', class_='product-tile')
        
#         individual_products = []
#         for tile in product_tiles:
#             individual_products.append(str(tile))
        
#         print(f"Found {len(individual_products)} product tiles in HTML")
#         return individual_products
    
#     def _extract_product_name(self, soup) -> str:
#         """Extract product name"""
#         name_element = soup.select_one('a.product-tile__text-link')
#         if name_element and name_element.get_text(strip=True):
#             return self.clean_text(name_element.get_text())
#         return "N/A"
    
#     def _extract_price(self, soup) -> str:
#         """Extract price information"""
#         price_element = soup.select_one('.pricing__retail .currency-format')
#         if price_element:
#             price_text = price_element.get_text(strip=True)
#             return self.extract_price_value(price_text)
        
#         price_element = soup.select_one('.currency-format')
#         if price_element:
#             price_text = price_element.get_text(strip=True)
#             return self.extract_price_value(price_text)
        
#         return "N/A"
    
#     def _extract_image(self, soup) -> str:
#         """Extract product image URL"""
#         img_element = soup.select_one('.product-tile__default-image')
#         if img_element and img_element.get('src'):
#             src = img_element.get('src')
#             return self._normalize_image_url(src)
#         return "N/A"
    
#     def _extract_link(self, soup) -> str:
#         """Extract product link"""
#         link_element = soup.select_one('a.product-tile__link')
#         if link_element and link_element.get('href'):
#             href = link_element.get('href')
#             return self._normalize_link_url(href)
#         return "N/A"
    
#     def _extract_diamond_weight(self, soup) -> str:
#         """Extract diamond weight from product name"""
#         product_name = self._extract_product_name(soup)
#         return self.extract_diamond_weight_value(product_name)
    
#     def _extract_gold_type(self, soup) -> str:
#         """Extract gold type from product name"""
#         product_name = self._extract_product_name(soup)
#         return self.extract_gold_type_value(product_name)
    
#     def _extract_badges(self, soup) -> list:
#         """Extract badge information"""
#         badges = []
#         badge_elements = soup.select('.product-tile__badge')
        
#         for badge in badge_elements:
#             badge_text = self.clean_text(badge.get_text())
#             if badge_text:
#                 badges.append(badge_text)
        
#         return badges
    
#     def _extract_promotions(self, soup) -> str:
#         """Extract promotion text"""
#         promo_el = soup.select_one('.product-tile__promotions .markdown')
#         if promo_el:
#             return self.clean_text(promo_el.get_text())
#         return "N/A"
    
#     def _normalize_image_url(self, url: str) -> str:
#         """Normalize image URL"""
#         if not url:
#             return "N/A"
#         if url.startswith('http'):
#             return url
#         elif url.startswith('/'):
#             return f"https://www.michaelhill.com.au{url}"
#         return url
    
#     def _normalize_link_url(self, url: str) -> str:
#         """Normalize link URL"""
#         if not url:
#             return "N/A"
#         if url.startswith('http'):
#             return url
#         elif url.startswith('/'):
#             return f"https://www.michaelhill.com.au{url}"
#         return url
    
#     def download_image_sync(self, image_url: str, product_name: str, timestamp: str, image_folder: str, unique_id: str) -> str:
#         """Download image synchronously"""
#         try:
#             if not image_url or image_url == "N/A":
#                 return "N/A"
            
#             # Clean product name for filename
#             clean_name = "".join(c for c in product_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
#             clean_name = clean_name[:50]
            
#             # Get file extension
#             parsed_url = urlparse(image_url)
#             file_ext = os.path.splitext(parsed_url.path)[1]
#             if not file_ext:
#                 file_ext = '.jpg'
            
#             # Generate filename
#             filename = f"{unique_id}_{clean_name}_{timestamp}{file_ext}"
#             filepath = os.path.join(image_folder, filename)
            
#             # Download image
#             response = requests.get(image_url, timeout=30)
#             if response.status_code == 200:
#                 with open(filepath, 'wb') as f:
#                     f.write(response.content)
#                 return filepath
#             else:
#                 return "N/A"
                
#         except Exception as e:
#             print(f"Error downloading image {image_url}: {e}")
#             return "N/A"
    
#     def clean_text(self, text: str) -> str:
#         """Clean and normalize text"""
#         if not text:
#             return ""
#         return ' '.join(text.split()).strip()
    
#     def extract_price_value(self, text: str) -> str:
#         """Extract price from text"""
#         if not text:
#             return "N/A"
#         price_match = re.search(r'(\$|€|£|¥|₹|Rs?\.?)\s*([\d,]+\.?\d*)', text)
#         return price_match.group(0) if price_match else "N/A"
    
#     def extract_diamond_weight_value(self, text: str) -> str:
#         """Extract diamond weight from text"""
#         if not text:
#             return "N/A"
#         weight_match = re.search(r'(\d+(?:\.\d+)?)\s*(?:ct|carat|carats)\s*(?:tw|total weight)?', text, re.IGNORECASE)
#         return f"{weight_match.group(1)} ct" if weight_match else "N/A"
    
#     def extract_gold_type_value(self, text: str) -> str:
#         """Extract gold type from text"""
#         if not text:
#             return "N/A"
#         gold_match = re.search(
#             r"(Yellow and White Gold|Yellow/White Gold|Yellow & White Gold|"
#             r"White and Yellow Gold|White/Yellow Gold|White & Yellow Gold|"
#             r"Rose Gold|White Gold|Yellow Gold|Platinum|Silver|"
#             r"\d{1,2}kt|\d{1,2}K)",
#             text,
#             re.IGNORECASE
#         )
#         return gold_match.group(0).title() if gold_match else "N/A"




import base64
import os
import uuid
from datetime import datetime
from bs4 import BeautifulSoup
import re
from typing import Dict, Any, List
import requests
from urllib.parse import urlparse
from openpyxl import Workbook
from database.db_inseartin import insert_into_db, update_product_count


class MichaelHillParser:
    """Parser for Michael Hill product pages with database and Excel functionality"""
    
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
            print("=================== Starting Michael Hill Parser ==================")
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
            
            # Create image folder for this session with absolute path
            image_folder_name = f"michaelhill_{timestamp}"
            image_folder_relative = os.path.join(self.image_save_path, image_folder_name)
            image_folder_absolute = os.path.abspath(image_folder_relative)
            os.makedirs(image_folder_absolute, exist_ok=True)
            
            print(f"Image folder created: {image_folder_absolute}")
            
            # Create Excel file with absolute path
            excel_filename = f"michaelhill_scraped_products_{timestamp}.xlsx"
            excel_path_relative = os.path.join(self.excel_data_path, excel_filename)
            excel_path_absolute = os.path.abspath(excel_path_relative)
            
            # Process products
            database_records = []
            successful_downloads = 0
            
            # Create Excel workbook
            wb = Workbook()
            sheet = wb.active
            sheet.title = "Michael Hill Products"
            
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
                    
                    # Download image and get full system path
                    image_url = parsed_data.get('image_url')
                    image_path_absolute = self.download_image_sync(
                        image_url, product_name, timestamp, image_folder_absolute, unique_id
                    )
                    
                    if image_path_absolute != "N/A":
                        successful_downloads += 1
                        print(f"Image saved to: {image_path_absolute}")
                    
                    # Prepare additional info
                    badges = parsed_data.get('badges', [])
                    promotions = parsed_data.get('promotions', '')
                    additional_info_parts = []
                    
                    if badges:
                        additional_info_parts.extend(badges)
                    if promotions and promotions != "N/A":
                        additional_info_parts.append(promotions)
                    
                    additional_info = " | ".join(additional_info_parts) if additional_info_parts else "N/A"
                    
                    # Create database record with full image path
                    db_record = {
                        'unique_id': unique_id,
                        'current_date': current_date,
                        'page_title': page_title,
                        'product_name': product_name,
                        'image_path': image_path_absolute,  # Full system path
                        'price': parsed_data.get('price'),
                        'diamond_weight': parsed_data.get('diamond_weight'),
                        'gold_type': parsed_data.get('gold_type'),
                        'additional_info': additional_info,
                    }
                    
                    database_records.append(db_record)
                    
                    # Add to Excel (store full path in Excel too)
                    sheet.append([
                        unique_id,
                        current_date.strftime('%Y-%m-%d'),
                        page_title,
                        product_name,
                        image_path_absolute,  # Full system path in Excel
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
            wb.save(excel_path_absolute)
            print(f"Excel file saved: {excel_path_absolute}")
            
            # Insert data into the database and update product count
            insert_into_db(database_records)
            update_product_count(len(database_records))
            
            # Encode Excel file to base64
            with open(excel_path_absolute, "rb") as file:
                base64_file = base64.b64encode(file.read()).decode("utf-8")
            
            # Return JSON response
            return {
                'message': f'Successfully processed {len(database_records)} products',
                'session_id': session_id,
                'excel_file': excel_filename,
                'total_processed': len(database_records),
                'images_downloaded': successful_downloads,
                'failed': len(individual_products) - len(database_records),
                'website_type': 'michaelhill',
                'base64_file': base64_file,
                'file_path': excel_path_absolute  # Return full path
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
        """Extract individual product HTML blocks"""
        if not html_content:
            return []
        
        soup = BeautifulSoup(html_content, 'html.parser')
        product_tiles = soup.find_all('div', class_='product-tile')
        
        individual_products = []
        for tile in product_tiles:
            individual_products.append(str(tile))
        
        print(f"Found {len(individual_products)} product tiles in HTML")
        return individual_products
    
    def _extract_product_name(self, soup) -> str:
        """Extract product name"""
        name_element = soup.select_one('a.product-tile__text-link')
        if name_element and name_element.get_text(strip=True):
            return self.clean_text(name_element.get_text())
        return "N/A"
    
    def _extract_price(self, soup) -> str:
        """Extract price information"""
        price_element = soup.select_one('.pricing__retail .currency-format')
        if price_element:
            price_text = price_element.get_text(strip=True)
            return self.extract_price_value(price_text)
        
        price_element = soup.select_one('.currency-format')
        if price_element:
            price_text = price_element.get_text(strip=True)
            return self.extract_price_value(price_text)
        
        return "N/A"
    
    def _extract_image(self, soup) -> str:
        """Extract product image URL"""
        img_element = soup.select_one('.product-tile__default-image')
        if img_element and img_element.get('src'):
            src = img_element.get('src')
            return self._normalize_image_url(src)
        return "N/A"
    
    def _extract_link(self, soup) -> str:
        """Extract product link"""
        link_element = soup.select_one('a.product-tile__link')
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
        """Extract badge information"""
        badges = []
        badge_elements = soup.select('.product-tile__badge')
        
        for badge in badge_elements:
            badge_text = self.clean_text(badge.get_text())
            if badge_text:
                badges.append(badge_text)
        
        return badges
    
    def _extract_promotions(self, soup) -> str:
        """Extract promotion text"""
        promo_el = soup.select_one('.product-tile__promotions .markdown')
        if promo_el:
            return self.clean_text(promo_el.get_text())
        return "N/A"
    
    def _normalize_image_url(self, url: str) -> str:
        """Normalize image URL"""
        if not url:
            return "N/A"
        if url.startswith('http'):
            return url
        elif url.startswith('/'):
            return f"https://www.michaelhill.com.au{url}"
        return url
    
    def _normalize_link_url(self, url: str) -> str:
        """Normalize link URL"""
        if not url:
            return "N/A"
        if url.startswith('http'):
            return url
        elif url.startswith('/'):
            return f"https://www.michaelhill.com.au{url}"
        return url
    
    def download_image_sync(self, image_url: str, product_name: str, timestamp: str, image_folder_absolute: str, unique_id: str) -> str:
        """Download image synchronously and return full system path"""
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
            filepath_absolute = os.path.join(image_folder_absolute, filename)
            
            print(f"Downloading image from: {image_url}")
            print(f"Saving image to: {filepath_absolute}")
            
            # Download image
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            response = requests.get(image_url, timeout=30, headers=headers)
            if response.status_code == 200:
                with open(filepath_absolute, 'wb') as f:
                    f.write(response.content)
                print(f"Image successfully saved: {filepath_absolute}")
                return filepath_absolute  # Return full absolute path
            else:
                print(f"Failed to download image. Status code: {response.status_code}")
                return "N/A"
                
        except Exception as e:
            print(f"Error downloading image {image_url}: {e}")
            return "N/A"
    
    def clean_text(self, text: str) -> str:
        """Clean and normalize text"""
        if not text:
            return ""
        return ' '.join(text.split()).strip()
    
    def extract_price_value(self, text: str) -> str:
        """Extract price from text"""
        if not text:
            return "N/A"
        price_match = re.search(r'(\$|€|£|¥|₹|Rs?\.?)\s*([\d,]+\.?\d*)', text)
        return price_match.group(0) if price_match else "N/A"
    
    def extract_diamond_weight_value(self, text: str) -> str:
        """Extract diamond weight from text"""
        if not text:
            return "N/A"
        weight_match = re.search(r'(\d+(?:\.\d+)?)\s*(?:ct|carat|carats)\s*(?:tw|total weight)?', text, re.IGNORECASE)
        return f"{weight_match.group(1)} ct" if weight_match else "N/A"
    
    def extract_gold_type_value(self, text: str) -> str:
        """Extract gold type from text"""
        if not text:
            return "N/A"
        gold_match = re.search(
            r"(Yellow and White Gold|Yellow/White Gold|Yellow & White Gold|"
            r"White and Yellow Gold|White/Yellow Gold|White & Yellow Gold|"
            r"Rose Gold|White Gold|Yellow Gold|Platinum|Silver|"
            r"\d{1,2}kt|\d{1,2}K)",
            text,
            re.IGNORECASE
        )
        return gold_match.group(0).title() if gold_match else "N/A"