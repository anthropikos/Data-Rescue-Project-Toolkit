from drptoolkit.esri import data_search
from drptoolkit.esri.data_scraper import FeatureServiceDataScraper
import logging
import sys
import pandas as pd

logger = logging.getLogger(__name__)
logging.basicConfig(
    level = logging.INFO,
    format = "%(asctime)s - %(levelname)s - %(message)s",
    # stream = sys.stdout,
    filemode="a",
    filename=f"{__name__}.log"
)

dc_arcgis_portal_url = "https://dcgis.maps.arcgis.com/"

if __name__ == "__main__":

    results = data_search(
        dc_arcgis_portal_url,
        "crime type:Feature Service", 
    )
    
    logger.info(f"Query results: {results}")

    results = pd.DataFrame(results)
    holder_download_status = []
    holder_exception_msg = []
    
    for idx, id in enumerate(results.id):

        logger.info(f"### Downloading {idx+1} of {results.shape[0]} datasets found.")
        
        try:
            ds = FeatureServiceDataScraper(id)
            ds.download()
            
            holder_download_status.append("Success")
            holder_exception_msg.append("None")
        except Exception as e:
            holder_download_status.append("Failed")
            holder_exception_msg.append(e)
            
        
    results["download_status"] = holder_download_status
    results["download_exception_msg"] = holder_exception_msg
    
    results.to_csv(f"{__file__}_download_status.csv")