import os
import re
import uuid
import logging
from datetime import datetime
from dotenv import load_dotenv
import pymssql

load_dotenv(override=True)

# Configure logging
logger = logging.getLogger(__name__)

# Database Configuration
DB_CONFIG = {
    "server": os.getenv("DB_SERVER"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    'port': 1433
}

def get_db_connection():
    """Create and return database connection"""
    try:
        conn = pymssql.connect(
            server=DB_CONFIG['server'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            database=DB_CONFIG['database'],
            port=DB_CONFIG['port']
        )
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise

def extract_metals(text):
    """Extracts and prioritizes longest, most descriptive metal-related phrases."""
    if not text:
        return []

    text_upper = text.upper()

    patterns = [
        # Match like "9K White Gold", "14CT Rose Gold & White Gold"
        r'\b\d{1,2}(?:K|CT|CARAT)\s*(?:WHITE|YELLOW|ROSE|STRAWBERRY|TWO[- ]TONE)?\s*GOLD\b',
        # Match standalone metal names
        r'\b(?:PLATINUM|STERLING\s*SILVER|SILVER|WHITE\s*GOLD|YELLOW\s*GOLD|ROSE\s*GOLD|STRAWBERRY\s*GOLD|TWO[- ]TONE\s*GOLD|TITANIUM|BRASS|PALLADIUM|COPPER|ALLOY)\b',
        # Match just karat values like "9K", "14CT" — only if not part of a decimal
        r'(?<![\d.])\b\d{1,2}(?:K|CT|CARAT)\b'
    ]

    all_matches = []
    for pattern in patterns:
        all_matches.extend(re.findall(pattern, text_upper))

    # Remove duplicates and substrings
    unique_matches = sorted(set(all_matches), key=len, reverse=True)
    final_matches = []
    for match in unique_matches:
        if not any(match in longer for longer in final_matches):
            final_matches.append(match)

    return final_matches

def extract_karat_info(text):
    """Extract karat information from text"""
    if not text:
        return None

    metals = extract_metals(text)
    if metals:
        value = metals[0].upper()

        # Normalize named metals
        for metal in ['PLATINUM', 'STERLING SILVER', 'SILVER', 'TITANIUM', 'BRASS', 'PALLADIUM', 'COPPER', 'ALLOY']:
            if metal in value:
                return metal.lower()

        # Normalize spacing and symbols
        if '&' in value:
            parts = [p.strip() for p in value.split('&')]
            value = ' & '.join(parts)

        # Fix concatenated names
        value = value.replace('WHITEGOLD', 'WHITE GOLD')
        value = value.replace('YELLOWGOLD', 'YELLOW GOLD')
        value = value.replace('ROSEGOLD', 'ROSE GOLD')
        value = value.replace('TWOTONE', 'TWO-TONE GOLD')
        value = value.replace('TWO TONE', 'TWO-TONE')

        # Convert CT/CARAT to K
        value = re.sub(r'(\d{1,2})(CT|CARAT)', r'\1K', value)
        value = re.sub(r'(\d{1,2}K)([A-Z])', r'\1 \2', value)

        return value.lower()

    # Fallback: check for "Diamond <Metal>" structure
    diamond_metal_match = re.search(
        r'\bDIAMOND\s+(PLATINUM|STERLING\s+SILVER|SILVER|TITANIUM|BRASS|PALLADIUM|COPPER|ALLOY)\b',
        text.upper()
    )
    if diamond_metal_match:
        return diamond_metal_match.group(1).lower()

    # Final fallback: If "diamond" is in the text but no metal found
    if "DIAMOND" in text.upper():
        return "diamond"

    return None

def parse_ct(val):
    """Convert ct string to float, supporting composite fractions like 1-3/4"""
    try:
        if '-' in val and '/' in val:
            whole, frac = val.split('-')
            num, denom = frac.split('/')
            return int(whole) + float(num) / float(denom)
        if '/' in val:
            num, denom = val.split('/')
            return float(num) / float(denom)
        return float(val)
    except Exception:
        return None

def standardize_diawt_value(value):
    """Standardize diamond weight format (e.g., '0.5ct tw')"""
    if not value:
        return None

    value = str(value).strip().lower()

    # Normalize slashes and spacing
    value = re.sub(r'\s*/\s*', '/', value)
    value = re.sub(r'\s+', ' ', value)

    # Detect if 'tw' (or variants) exist
    has_tw = any(tw in value for tw in [' tw', 'tw', 't.w.', 'ctw'])

    # Extract the number portion (e.g., 0.5, 3/4, 1-1/2)
    num_match = re.search(r'(\d+-\d+/\d+|\d+/\d+|\d*\.\d+|\d+)', value)
    if not num_match:
        return None

    num_part = num_match.group(1)
    return f"{num_part}ct tw" if has_tw else f"{num_part}ct"

def extract_diamond_weight(text):
    """Extract smallest valid diamond weight (ct), preserving 'tw' and handling formats like 0,50 ct"""
    if not text:
        return None

    text = str(text).upper()

    # Convert European decimal (comma) to dot
    text = text.replace(',', '.')

    # Remove metal descriptors only if at start
    metal_free_text = re.sub(
        r'^\s*\d{1,2}(?:K|CT|CARAT)(?:\s*(?:[A-Z]+\s*&\s*[A-Z]+|ROSE|WHITE|YELLOW|STRAWBERRY|TWO-TONE)\s*GOLD)?\s*',
        '',
        text,
        flags=re.IGNORECASE
    )

    if any(x in metal_free_text for x in ['CUBIC ZIRCONIA', 'SAPPHIRE', 'CREATED']):
        return None

    # Match patterns: 1-3/4, 3/4, 0.25, 0,50, 1.25, etc. with ct indicators
    matches = re.findall(
        r'(\d+-\d+/\d+|\d+/\d+|\d*\.\d+|\d+)\s*(CTW|CT\s*TW|CT|CARAT\s*TW|CARAT|CT\.*\s*T*W*\.?)',
        metal_free_text,
        re.IGNORECASE
    )

    diamond_cts = []
    for val, unit in matches:
        ct_val = parse_ct(val)
        if ct_val is not None and ct_val < 5.0:
            diamond_cts.append((val.strip(), unit.strip(), ct_val))

    if not diamond_cts:
        return None

    # Pick the smallest valid ct
    smallest = min(diamond_cts, key=lambda x: x[2])
    return standardize_diawt_value(f"{smallest[0]} {smallest[1]}")



def process_row(row):
    """Process individual row data for database insertion"""
    try:
        unique_id = row.get('unique_id', str(uuid.uuid4()))
        current_date = row.get('current_date', datetime.now().date())
        header = row.get('page_title', '')[:500]
        product_name = row.get('product_name', '')[:500]
        image_path = row.get('image_path', '')[:1000]
        
        # Enhanced Kt extraction
        kt = extract_karat_info(row.get('product_name', ''))
        
        # Price extraction
        price = row.get('price', '')
        
        # Enhanced diamond weight extraction
        total_dia_wt = extract_diamond_weight(row.get('product_name', ''))
        
        additional_info = row.get('additional_info', '')[:1000] if row.get('additional_info') else None
        
        return (
            unique_id,
            current_date,
            header,
            product_name,
            image_path,
            kt,
            price,
            total_dia_wt,
            additional_info
        )
    except Exception as e:
        logger.error(f"Error processing row: {e}")
        return None

def insert_into_db(data):
    """Insert scraped data into the MSSQL database"""
    if not data:
        logger.warning("No data to insert into the database.")
        return
    
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            query = """
                INSERT INTO dbo.IBM_Algo_Webstudy_Products 
                (unique_id, CurrentDate, Header, ProductName, ImagePath, Kt, Price, TotalDiaWt, AdditionalInfo)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            processed_data = [process_row(row) for row in data]
            # Filter out None values
            processed_data = [row for row in processed_data if row is not None]
            
            if processed_data:
                cursor.executemany(query, processed_data)
                conn.commit()
                logger.info(f"Inserted {len(processed_data)} records successfully.")
            else:
                logger.warning("No valid data to insert after processing.")
        
    except pymssql.DatabaseError as e:
        logger.error(f"Database error: {e}")
        if conn:
            conn.rollback()
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

def update_product_count(count):
    """Update monthly product count in the database"""
    if count <= 0:
        return
        
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                UPDATE IBM_Algo_Webstudy_scraping_settings 
                SET products_fetched_month = products_fetched_month + %s
                WHERE setting_name = 'monthly_product_limit'
            """, (count,))
            conn.commit()
            logger.info(f"Updated monthly product count by +{count}")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error updating monthly count: {e}")
        raise
    finally:
        conn.close()