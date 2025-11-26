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
import httpx
from urllib.parse import urlparse
from openpyxl import Workbook
from database.db_inseartin import insert_into_db, update_product_count

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

IMAGE_SAVE_PATH = os.getenv("IMAGE_SAVE_PATH")
EXCEL_DATA_PATH = os.getenv("EXCEL_DATA_PATH")


class HoskingsScraper:
    """Parser for Hoskings.com.au Shopify store with database and Excel functionality"""
    
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
            print("=================== Starting Hoskings Scraper ==================")
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
            image_folder = os.path.join(self.image_save_path, f"hoskings_{timestamp}")
            os.makedirs(image_folder, exist_ok=True)
            
            # Create Excel file
            excel_filename = f"hoskings_scraped_products_{timestamp}.xlsx"
            excel_path = os.path.join(self.excel_data_path, excel_filename)
            
            # Process products
            database_records = []
            successful_downloads = 0
            
            # Create Excel workbook
            wb = Workbook()
            sheet = wb.active
            sheet.title = "Hoskings Products"
            
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
                    
                    # Skip if essential data is missing
                    if (parsed_data.get('product_name') == "N/A" or 
                        parsed_data.get('image_url') == "N/A"):
                        print(f"Skipping product due to missing data: Name: {parsed_data.get('product_name')}, Image: {parsed_data.get('image_url')}")
                        continue
                    
                    # Generate unique ID
                    unique_id = str(uuid.uuid4())
                    product_name = parsed_data.get('product_name', 'Unknown Product')[:495]
                    
                    # Download image - use async method
                    image_url = parsed_data.get('image_url')
                    image_path = asyncio.run(self.download_image_async(
                        image_url, product_name, timestamp, image_folder, unique_id
                    ))
                    
                    if image_path != "N/A":
                        successful_downloads += 1
                    
                    # Prepare additional info
                    badges = parsed_data.get('badges', [])
                    promotions = parsed_data.get('promotions', '')
                    additional_info_parts = []
                    
                    if badges and badges != ["N/A"]:
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
                'website_type': 'hoskings',
                'base64_file': base64_file,
                'file_path': excel_path
            }
            
        except Exception as e:
            print(f"Error in parse_and_save_products: {e}")
            return {
                'error': str(e),
                'message': 'Failed to process products'
            }

    def scrape_shopify_json(self, base_url: str) -> List[Dict]:
        """
        Shopify-specific scraping using the JSON API endpoint
        This is much more efficient than HTML parsing
        """
        try:
            # Construct the Shopify products.json URL
            json_url = f"{base_url.rstrip('/')}/products.json"
            print(f"Fetching Shopify data from: {json_url}")
            
            response = requests.get(json_url, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            products = data.get('products', [])
            
            print(f"Found {len(products)} products via Shopify API")
            
            parsed_products = []
            for product in products:
                # Get the first variant for pricing
                variants = product.get('variants', [])
                first_variant = variants[0] if variants else {}
                
                # Get the first image
                images = product.get('images', [])
                first_image = images[0] if images else {}
                
                parsed_products.append({
                    'product_name': product.get('title', 'N/A'),
                    'price': f"${first_variant.get('price', 'N/A')}",
                    'image_url': first_image.get('src', 'N/A'),
                    'link': f"{base_url.rstrip('/')}/products/{product.get('handle', '')}",
                    'diamond_weight': self._extract_diamond_weight(product.get('title', '')),
                    'gold_type': self._extract_gold_type(product.get('title', '')),
                    'badges': ["SALE"] if first_variant.get('compare_at_price') else ["N/A"],
                    'promotions': f"Was ${first_variant.get('compare_at_price', 'N/A')}" if first_variant.get('compare_at_price') else "N/A"
                })
            
            return parsed_products
            
        except Exception as e:
            print(f"Error scraping Shopify JSON: {e}")
            return []
    
    def parse_product(self, product_html: str) -> Dict[str, Any]:
        """Parse individual product HTML from Hoskings Shopify store"""
        try:
            soup = BeautifulSoup(product_html, 'html.parser')
            
            product_name = self._extract_product_name(soup)
            price = self._extract_price(soup)
            image_url = self._extract_image(soup)
            link = self._extract_link(soup)
            
            # Skip if essential data is missing
            if product_name == "N/A" or image_url == "N/A":
                return {
                    'product_name': product_name,
                    'price': price,
                    'image_url': image_url,
                    'link': link,
                    'diamond_weight': "N/A",
                    'gold_type': "N/A",
                    'badges': ["N/A"],
                    'promotions': "N/A"
                }
            
            return {
                'product_name': product_name,
                'price': price,
                'image_url': image_url,
                'link': link,
                'diamond_weight': self._extract_diamond_weight(product_name),
                'gold_type': self._extract_gold_type(product_name),
                'badges': self._extract_badges(soup),
                'promotions': self._extract_promotions(soup)
            }
        except Exception as e:
            logger.error(f"Error parsing product: {e}")
            return {
                'product_name': "N/A",
                'price': "N/A",
                'image_url': "N/A",
                'link': "N/A",
                'diamond_weight': "N/A",
                'gold_type': "N/A",
                'badges': ["N/A"],
                'promotions': "N/A"
            }
    
    def extract_individual_products_from_html(self, html_content: str) -> List[str]:
        """Extract individual product HTML blocks from Hoskings HTML"""
        if not html_content:
            return []
        
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Hoskings specific product selectors (Shopify store)
        product_selectors = [
            'div.w-full.cursor-pointer.relative',  # Main product container
            '.infiniteHits div.w-full',  # Products in infinite hits
            'div[class*="cursor-pointer"]',  # Any cursor pointer divs
            '.grid > div'  # Direct children of grid
        ]
        
        individual_products = []
        
        for selector in product_selectors:
            product_tiles = soup.select(selector)
            for tile in product_tiles:
                individual_products.append(str(tile))
            if individual_products:
                break  # Stop if we found products with this selector
        
        print(f"Found {len(individual_products)} product tiles in Hoskings HTML")
        return individual_products
    
    def _extract_product_name(self, soup) -> str:
        """Extract product name from Hoskings product tile"""
        # Try multiple selectors for product name
        name_selectors = [
            'p.font-normal.text-text-subdued.text-label',  # Product description
            'a p.font-normal',  # Paragraph in link
            '.text-label',  # Any label text
            'p:contains("ct")'  # Paragraph containing "ct" (carat)
        ]
        
        for selector in name_selectors:
            name_element = soup.select_one(selector)
            if name_element and name_element.get_text(strip=True):
                return self.clean_text(name_element.get_text())
        
        return "N/A"
    
    def _extract_price(self, soup) -> str:
        """Extract price information from Hoskings product"""
        try:
            # Hoskings sale price may use multiple possible classes
            sale_price_element = soup.select_one(
                'span.text-text-sale, span.text-promo-green, span.text-promo-green.pr-1, span.text-text-sale.pr-1'
            )
            
            # Original price (line-through)
            original_price_element = soup.select_one('span.line-through.text-text-disabled')

            sale_price = sale_price_element.get_text(strip=True) if sale_price_element else ""
            original_price = original_price_element.get_text(strip=True) if original_price_element else ""

            if sale_price and original_price:
                return f"{sale_price} | {original_price}"
            elif sale_price:
                return sale_price
            elif original_price:
                return original_price

            # fallback
            price_container = soup.select_one('div.text-label.text-text-subdued.font-bold.mt-1')
            if price_container:
                price_text = price_container.get_text(strip=True)
                return self.extract_price_value(price_text)

            return "N/A"
        
        except Exception as e:
            logger.warning(f"Error extracting price: {e}")
            return "N/A"

    def _extract_image(self, soup) -> str:
        """Extract product image URL from Hoskings product"""
        try:
            # Hoskings uses Shopify CDN images
            img_element = soup.select_one('img[src*="cdn.shopify.com"]')
            if img_element:
                image_url = img_element.get('src') or img_element.get('data-src')
                if image_url:
                    return self._normalize_image_url(image_url)
            
            return "N/A"
        except Exception as e:
            logger.warning(f"Error extracting image: {e}")
            return "N/A"
    
    def _extract_link(self, soup) -> str:
        """Extract product link from Hoskings product"""
        # Link selectors for Hoskings
        link_selectors = [
            'a[href*="/products/"]',  # Product links
            'a[href^="/products"]',  # Relative product links
            '.relative a'  # Links in relative container
        ]
        
        for selector in link_selectors:
            link_element = soup.select_one(selector)
            if link_element and link_element.get('href'):
                href = link_element.get('href')
                return self._normalize_link_url(href)
        
        return "N/A"
    
    def _extract_diamond_weight(self, product_name: str) -> str:
        """Extract diamond weight from product name"""
        if not product_name or product_name == "N/A":
            return "N/A"
        
        # Hoskings uses formats like "TDW 1CT", "TDW 2CT"
        diamond_patterns = [
            r"TDW\s*(\d+(?:\.\d+)?)\s*CT",  # TDW 1CT, TDW 2CT
            r"(\d+(?:\.\d+)?)\s*CT\s+TW",  # 1CT TW
            r"(\d+(?:\.\d+)?)\s*carat",  # 1 carat
            r"(\d+(?:\.\d+)?)\s*ct"  # 1 ct
        ]
        
        for pattern in diamond_patterns:
            diamond_match = re.search(pattern, product_name, re.IGNORECASE)
            if diamond_match:
                weight = diamond_match.group(1)
                return f"{weight} CT"
        
        return "N/A"
    
    def _extract_gold_type(self, product_name: str) -> str:
        """Extract gold type from product name"""
        if not product_name or product_name == "N/A":
            return "N/A"
        
        # Hoskings uses formats like "14ct Rose Gold", "14ct Yellow Gold"
        gold_patterns = [
            r"\b\d{1,2}ct\s+(?:rose|yellow|white)\s+gold\b",
            r"\b(?:9ct|14ct|18ct)\s+(?:rose|yellow|white)\s+gold\b",
            r"\b(?:rose|yellow|white)\s+gold\b",
            r"\bsterling\s+silver\b",
            r"\bplatinum\b"
        ]
        
        for pattern in gold_patterns:
            gold_type_match = re.search(pattern, product_name, re.IGNORECASE)
            if gold_type_match:
                return gold_type_match.group().upper()
        
        return "N/A"
    
    def _extract_badges(self, soup) -> list:
        """Extract badge information from Hoskings product"""
        badges = []
        
        try:
            # Extract sale badges
            badge_elements = soup.select('div.absolute.z-10.bg-brand-polar-white')
            for badge_element in badge_elements:
                badge_text = self.clean_text(badge_element.get_text())
                if badge_text and badge_text.upper() == "SALE":
                    badges.append("SALE")
        except Exception as e:
            logger.warning(f"Error extracting badges: {e}")
        
        return badges if badges else ["N/A"]
    
    def _extract_promotions(self, soup) -> str:
        """Extract promotion text from Hoskings product"""
        # For Hoskings, the original price serves as promotion info
        original_price_element = soup.select_one('span.line-through.text-text-disabled')
        if original_price_element:
            original_price = original_price_element.get_text(strip=True)
            return f"Was {original_price}" if original_price else "N/A"
        
        return "N/A"
    
    def _normalize_image_url(self, url: str) -> str:
        """Normalize image URL for Hoskings"""
        if not url or url == "N/A":
            return "N/A"
        if url.startswith('http'):
            return url
        elif url.startswith('//'):
            return f"https:{url}"
        elif url.startswith('/'):
            return f"https://hoskings.com.au{url}"
        return url
    
    def _normalize_link_url(self, url: str) -> str:
        """Normalize link URL for Hoskings"""
        if not url or url == "N/A":
            return "N/A"
        if url.startswith('http'):
            return url
        elif url.startswith('//'):
            return f"https:{url}"
        elif url.startswith('/'):
            return f"https://hoskings.com.au{url}"
        return url

    def modify_image_url(self, image_url: str) -> str:
        """Modify the image URL to request higher resolution for Shopify CDN."""
        if not image_url or image_url == "N/A":
            return image_url

        # Shopify CDN images - you can modify parameters for higher resolution
        # Common parameters: width, height, crop, scale
        if "cdn.shopify.com" in image_url:
            if "?" in image_url:
                return image_url + "&width=1000"
            else:
                return image_url + "?width=1000"
        
        return image_url

    async def download_image_async(self, image_url, product_name, timestamp, image_folder, unique_id, retries=3):
        """Async image download with high-resolution preference"""
        if not image_url or image_url == "N/A":
            return "N/A"

        image_filename = f"{unique_id}_{timestamp}.jpg"
        image_full_path = os.path.join(image_folder, image_filename)

        high_res_url = self.modify_image_url(image_url)

        async with httpx.AsyncClient(timeout=30.0) as client:
            # Try high-resolution first
            for attempt in range(retries):
                try:
                    response = await client.get(high_res_url)
                    response.raise_for_status()
                    
                    # Verify it's actually an image
                    content_type = response.headers.get('content-type', '')
                    if not content_type.startswith('image/'):
                        logger.warning(f"URL {high_res_url} returned non-image content type: {content_type}")
                        continue
                        
                    with open(image_full_path, "wb") as f:
                        f.write(response.content)
                    
                    logger.info(f"Successfully downloaded high-res image for {product_name}")
                    return image_full_path
                    
                except httpx.RequestError as e:
                    logger.warning(f"Retry {attempt + 1}/{retries} - High-res failed for {product_name}: {e}")
                    if attempt < retries - 1:
                        await asyncio.sleep(2)  # Wait before retry
            
            # Fallback to original image
            try:
                response = await client.get(image_url)
                response.raise_for_status()
                
                # Verify it's actually an image
                content_type = response.headers.get('content-type', '')
                if not content_type.startswith('image/'):
                    logger.warning(f"Original URL {image_url} returned non-image content type: {content_type}")
                    return "N/A"
                    
                with open(image_full_path, "wb") as f:
                    f.write(response.content)
                
                logger.info(f"Successfully downloaded original image for {product_name}")
                return image_full_path
                
            except httpx.RequestError as e:
                logger.error(f"Fallback failed for {product_name}: {e}")
                return "N/A"
    
    def extract_price_value(self, text: str) -> str:
        """Extract price from text"""
        if not text:
            return "N/A"
        
        # Look for price patterns (Australian dollar format)
        price_patterns = [
            r'\$\s*[\d,]+\.?\d*',  # Dollar price format
            r'\$\s*\d{1,3}(?:,\d{3})*(?:\.\d{2})?',  # Formatted dollar price
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
