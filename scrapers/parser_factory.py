from urllib.parse import urlparse
from scrapers.anguscoote import AngusCooteScraper
from scrapers.fields import FieldsScraper
from scrapers.goldmark import GoldmarkScraper
from scrapers.hoskings import HoskingsScraper
from scrapers.prouds import ProudsScraper
from scrapers.bulgari import BulgariScraper
from scrapers.chanel import ChanelScraper
from scrapers.chaumet import ChaumetScraper
from scrapers.fredmeyerjewelers import FredMeyerJewelersParser
from scrapers.jcpenney import JCPenneyParser
from scrapers.kay import KayParser
from scrapers.kayoutlet import KayOutletParser
from scrapers.louisvuitton import LouisVuittonScraper
from scrapers.macys import MacysParser
from scrapers.peoplesjewellers import PeoplesJewellersParser
from scrapers.shaneco import ShaneCoScraper
from scrapers.tiffany import TiffanyScraper
from scrapers.vancleefarpels import VanCleefArpelsScraper
from scrapers.zales import ZalesParser
from scrapers.michaelhill import MichaelHillParser
from scrapers.jared import JaredParser


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
        elif 'www.kay.com' in website_type:
            # return KayParser()  # Uncomment when KayParser parser is implemented
            return KayParser()  # Fallback for now
        elif 'www.zales.com' in website_type:
            # return ZalesParser()  # Uncomment when ZalesParser parser is implemented
            return ZalesParser()  # Fallback for now
        elif 'www.kayoutlet.com' in website_type:
            # return KayOutletParser()  # Uncomment when kylayoutparser parser is implemented
            return KayOutletParser()  # Fallback for now
        elif 'www.fredmeyerjewelers.com' in website_type:
            # return FredMeyerJewelersParser()  # Uncomment when FredMeyerJewelersParser parser is implemented
            return FredMeyerJewelersParser()  # Fallback for now
        elif 'www.jcpenney.com' in website_type:
            # return JCPenneyParser()  # Uncomment when JCPenneyParser parser is implemented
            return JCPenneyParser()  # Fallback for now
        elif 'www.macys.com' in website_type:
            # return MacysParser()  # Uncomment when MacysParser parser is implemented
            return MacysParser()  # Fallback for now
        elif 'www.peoplesjewellers.com' in website_type:
            # return PeoplesJewellersParser()  # Uncomment when PeoplesJewellersParser parser is implemented
            return PeoplesJewellersParser()  # Fallback for now
        elif 'www.shaneco.com' in website_type:
            # return ShaneCoScraper()  # Uncomment when ShaneCoScraper parser is implemented
            return ShaneCoScraper()  # Fallback for now
        elif 'www.tiffany.com' in website_type:
            # return TiffanyScraper()  # Uncomment when TiffanyScraper parser is implemented
            return TiffanyScraper()  # Fallback for now
        
        elif 'www.chanel.com' in website_type:
            # return ChanelScraper()  # Uncomment when ChanelScraper parser is implemented
            return ChanelScraper()  # Fallback for now
        elif 'www.chaumet.com' in website_type:
            # return ChaumetScraper()  # Uncomment when ChaumetScraper parser is implemented
            return ChaumetScraper()  # Fallback for now

        elif 'www.vancleefarpels.com' in website_type:
            # return VanCleefArpelsScraper()  # Uncomment when VanCleefArpelsScraper parser is implemented
            return VanCleefArpelsScraper()  # Fallback for now
        
        elif 'www.bulgari.com' in website_type:
            # return BulgariScraper()  # Uncomment when BulgariScraper parser is implemented
            return BulgariScraper()  # Fallback for now
        
        elif 'in.louisvuitton.com' in website_type:
            # return BulgariScraper()  # Uncomment when BulgariScraper parser is implemented
            return LouisVuittonScraper()  # Fallback for now
        
        elif 'www.prouds.com.au' in website_type:
            # return ProudsScraper()  # Uncomment when ProudsScraper parser is implemented
            return ProudsScraper()  # Fallback for now
        
        elif 'www.goldmark.com.au' in website_type:
            # return GoldmarkScraper()  # Uncomment when GoldmarkScraper parser is implemented
            return GoldmarkScraper()  # Fallback for now
        
        elif 'www.anguscoote.com.au' in website_type:
            # return AngusCooteScraper()  # Uncomment when AngusCooteScraper parser is implemented
            return AngusCooteScraper()  # Fallback for now
        
        elif 'www.fields.ie' in website_type:
            # return FieldsScraper()  # Uncomment when FieldsScraper parser is implemented
            return FieldsScraper()  # Fallback for now
        
        elif 'hoskings.com.au' in website_type:
            # return HoskingsScraper()  # Uncomment when HoskingsScraper parser is implemented
            return HoskingsScraper()  # Fallback for now
        
        else:
            # Default to unknown  parser for unknown sites
            return 'unknown'
    
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
        elif 'www.kay.com' in domain:
            return 'www.kay.com'
        elif 'www.zales.com' in domain:
            return 'www.zales.com'
        elif 'www.kayoutlet.com' in domain:
            return 'www.kayoutlet.com'
        elif 'www.fredmeyerjewelers.com' in domain:
            return 'www.fredmeyerjewelers.com'
        elif 'www.jcpenney.com' in domain:
            return 'www.jcpenney.com'
        elif 'www.macys.com' in domain:
            return 'www.macys.com'
        elif 'www.peoplesjewellers.com' in domain:
            return 'www.peoplesjewellers.com'
        elif 'www.shaneco.com' in domain:
            return 'www.shaneco.com'
        elif 'www.tiffany.com' in domain:
            return 'www.tiffany.com'
        elif 'www.chanel.com' in domain:
            return 'www.chanel.com'
        elif 'www.chaumet.com' in domain:
            return 'www.chaumet.com'
        elif 'www.vancleefarpels.com' in domain:
            return 'www.vancleefarpels.com'
        elif 'www.bulgari.com' in domain:
            return 'www.bulgari.com'
        elif 'in.louisvuitton.com' in domain:
            return 'in.louisvuitton.com'
        elif 'www.prouds.com.au' in domain:
            return 'www.prouds.com.au'
        elif 'www.goldmark.com.au' in domain:
            return 'www.goldmark.com.au'
        elif 'www.anguscoote.com.au' in domain:
            return 'www.anguscoote.com.au'
        elif 'www.fields.ie' in domain:
            return 'www.fields.ie'
        elif 'hoskings.com.au' in domain:
            return 'hoskings.com.au'
        else:
            return 'unknown'