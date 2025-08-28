from drptoolkit.esri import FeatureServiceDataScraper
import requests
from pprint import pprint
import json
import logging
import sys

# logger = logging.getLogger(__name__)
logging.basicConfig(
    level = logging.INFO,
    format = "%(asctime)s - %(levelname)s - %(message)s",
    # filename = f"{__name__}.log",
    # filemode = 'a',
    stream = sys.stdout,
)

# Searching the ArcGIS portal 
dc_arcgis_portal_url = "https://dcgis.maps.arcgis.com/"
rest_api_url="https://maps2.dcgis.dc.gov/dcgis/rest/services/FEEDS/MPD/FeatureServer/7"
esri_id = "74d924ddc3374e3b977e6f002478cb9b"

if __name__ == "__main__":
    # data_scraper = FeatureServiceDataScraper(esri_id=esri_id)
    # data_scraper.download()
    
    pass