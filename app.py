import logging
from datetime import datetime
from flask import Flask, request, jsonify
from flask_cors import CORS
from scrapers.parser_factory import ParserFactory

app = Flask(__name__)
CORS(app)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def log_event(message):
    """Log events with timestamp"""
    logger.info(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {message}")

@app.route('/api/scrape/save', methods=['POST'])
def save_scraped_data():
    """Save scraped product data to database and generate Excel file"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({'error': 'No data provided'}), 400

        products_data = data.get('products', [])
        page_title = data.get('page_title', 'Unknown Page')
        pageUrl = data.get('pageUrl', 'Unknown Page')

        print("=================== Received Data ==================")
        print(f"Products count: {len(products_data)}")
        print(f"Page title: {page_title}")
        print(f"Page URL: {pageUrl}")
        print("=================== Received Data ==================")

        if not products_data:
            return jsonify({'error': 'No products data provided'}), 400

        # Extract HTML content from the first entry
        html_content = products_data[0].get('html', '') if products_data else ''
        
        print("=================== HTML Content ==================")
        print(f"HTML length: {len(html_content)} characters")
        print("First 500 chars:", html_content[:500] if html_content else "No HTML content")
        print("=================== HTML Content ==================")

        # Use page URL to detect website and create appropriate parser
        website_type = ParserFactory.detect_website(pageUrl)
        parser = ParserFactory.create_parser(website_type)
        
        print(f"Detected website: {website_type} from URL: {pageUrl}")

        # Let the parser handle everything
        result = parser.parse_and_save_products(products_data, page_title, pageUrl)
        
        if 'error' in result:
            return jsonify({'error': result['error']}), 500
        
        return jsonify(result), 200

    except Exception as e:
        logger.error(f"Error saving scraped data: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({'status': 'healthy', 'timestamp': datetime.now().isoformat()})

if __name__ == '__main__':
    logger.info("Starting Smart Scraper Backend Server...")
    app.run(debug=True, host='0.0.0.0', port=5000)