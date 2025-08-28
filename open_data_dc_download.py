from drptoolkit.esri import data_search
from drptoolkit.esri.data_scraper import FeatureServiceDataScraper
import logging
import sys
import pandas as pd
from pathlib import Path
from drptoolkit.utilities.time_utility import get_iso_time_str

logger = logging.getLogger(__name__)

logging.basicConfig(
    level = logging.INFO,
    format = "%(asctime)s - %(levelname)s - %(message)s",
    filemode="a",
    filename=f"{__file__}_{get_iso_time_str()}.log"
)

dc_arcgis_portal_url = "https://dcgis.maps.arcgis.com/"
download_path = Path().home() / Path("Downloads")
csv_path = ( download_path / Path("Open Data DC.csv")).resolve()
output_dir = ( download_path / Path("open_data_dc_esri_downloads")).resolve()

if __name__ == "__main__":

    results = pd.read_csv(csv_path)

    holder_download_status = []
    holder_exception_msg = []
    
    for idx, id in enumerate(results.id):
        
        try:
            ds = FeatureServiceDataScraper(id)
            ds.download(data_path=output_dir)

            logger.info(f"### Downloading {idx+1} of {results.shape[0]} datasets - ArcGIS ID - {id} - Success")
            
            holder_download_status.append("Success")
            holder_exception_msg.append("None")
        except Exception as error_msg:
            logger.info(f"### Downloading {idx+1} of {results.shape[0]} datasets - ArcGIS ID - {id} - Failed - {error_msg}")

            holder_download_status.append("Failed")
            holder_exception_msg.append(error_msg)
            
        
    results["download_status"] = holder_download_status
    results["download_exception_msg"] = holder_exception_msg
    
    results.to_csv(output_dir / Path(f"{__file__}_download_status.csv"))