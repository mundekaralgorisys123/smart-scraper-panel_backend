from urllib.parse import urlparse
from .michaelhill import MichaelHillParser
from .jared import JaredParser
class ParserFactory:
    """Factory to create appropriate parser based on website"""
    
    @staticmethod
    def create_parser(website_type: str):
        """Create parser based on website type"""
        if 'www.michaelhill.com.au' in website_type:
            return MichaelHillParser()
        elif 'www.jared.com' in website_type:
            # return JaredParser()  # Uncomment when Jared parser is implemented
            return JaredParser()  # Fallback for now
        else:
            # Default to Michael Hill parser for unknown sites
            return MichaelHillParser()
    
    @staticmethod
    def detect_website(website_url: str) -> str:
        """Detect website from URL"""
        if not website_url:
            return 'unknown'
            
        domain = urlparse(website_url).netloc.lower()
        
        if 'www.michaelhill.com.au' in domain:
            return 'www.michaelhill.com.au'
        elif 'www.jared.com' in domain:
            return 'www.jared.com'
        else:
            return 'unknown'