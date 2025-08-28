"""
Toolkit collection for the ESRI ArcGIS REST API.
https://developers.arcgis.com/rest/
"""
from pathlib import Path
from arcgis.gis import GIS
import requests
from datetime import date
import json
from .data_scraper import FeatureServiceDataScraper

def data_search(arcgis_portal_url, search_term, item_count_limit=100):
    gis = GIS(arcgis_portal_url)

    items = gis.content.search(
        query = f"{search_term}",
        sort_field = "title", 
        sort_order = "asc",
        max_items = item_count_limit,
    )
    
    print(f"### Returning the top {item_count_limit} results: ###")
    for idx, item in enumerate(items):
        result = f"""{idx} - {item}
        ID: {item.id}
        Title: {item.title}
        ArcGIS Online Page: {item.homepage}
        REST API URL: {item.url}"""
    
        print(result, end="\n")
    
    return items