import requests
from urllib import parse
from pathlib import Path
import logging
import json
import sys
from typing import Union
from arcgis.gis import GIS

logger = logging.getLogger(__name__)

# Types of GIS services: https://enterprise.arcgis.com/en/server/latest/publish-services/windows/what-types-of-services-can-you-publish.htm

class FeatureServiceDataScraper():
    """
    Feature Services: "Feature Services allow you to serve feature data and nonspatial tables over the internet or your
    intranet. This makes your data available for use in web clients, desktop apps, and field apps." [^1]

    References: 
    [^1]: ESRI ArcGIS documentation: https://enterprise.arcgis.com/en/server/latest/publish-services/windows/what-is-a-feature-service-.htm
    """
    
    def __init__(self, esri_id):
        # URL to map service you want to extract data from
        self.esri_id = esri_id
        self.service_url = self.__check_feature_server_or_feature_layer_url(self.__get_rest_api_url())
        self.service_prop = self.__get_service_properties()
        self.max_record_count = self.service_prop["maxRecordCount"]
        self.data_name = self.service_prop["name"]

    def __check_feature_server_or_feature_layer_url(self, rest_api_url:str) -> str:
        """When the REST API URL points to a feature server, we need to find the layer ID and update the url"""

        parsed_path = parse.urlparse(rest_api_url).path
        last_element = parsed_path.split('/')[-1]

        try: 
            last_element_int = int(last_element)
            return rest_api_url
        except ValueError:
            # Doesn't have ID thus a feature server
            service_prop = requests.get(
                rest_api_url,
                params = {"f": "json"},
            ).json()
            
            layers_list = service_prop["layers"]
            
            if len(layers_list) > 1: 
                raise ValueError(f"Feature Server has more than one layer! Got {len(layers_list)} layers")

            layer_id = layers_list[0]["id"]
            
            return f"{rest_api_url}/{layer_id}"

        

    
    def __get_rest_api_url(self,) -> str:
        gis = GIS()
        result = gis.content.search(query=f"id:{self.esri_id}")
        
        if len(result) > 1:
            raise ValueError(f"Found more than one results, expected one result only.")

        # title = result[0].title
        # arcgis_homepage = result[0].homepage
        rest_api_url = result[0].url
        
        return rest_api_url

    
    def __get_service_properties(self,) -> dict:
        """Get the service server's properties.
        
        A GET request at the REST API endpoint returns the properties of the service server, such as the service type, 
        `MaxRecordCount`, `GeometryType`, `Supported Query Formats`, etc.
        """
        response = requests.get(
            url = self.service_url,
            params = {"f": "json"},
        )

        return response.json()


    def __get_all_data_ids(self,) -> list:
        """Get all the ESRI Object IDs for the data on the server.
        
        ObjectID are added to tables created using ArcGIS thus they aren't inherit to the data [^1]. This unique ID is 
        used to keep track of the incremental downloads we make. Additionally, "there is no limit to the number of
        object IDs returned in the ID array response" [^2]. 
        
        
        Reference: 
        [^1]: ESRI ArcGIS documentation: https://desktop.arcgis.com/en/arcmap/latest/manage-data/using-sql-with-gdbs/object-id.htm
        [^2]: ArcGIS REST documentation - `returnIdsOnly` circumvents the MaxRecordCount limitation: https://developers.arcgis.com/rest/services-reference/enterprise/query-feature-service-layer/
        
        """

        url = self.service_url + "/query"
        params = {
            'f':'json', 
            'returnIdsOnly': True,  # No record count limit
            'where' : '1=1',
        }

        response = requests.get(
            url = url, 
            params = params,
        )
        id_list = response.json()["objectIds"]

        return id_list



    def __download_data_by_id_list(self, id_list:list):

        if len(id_list) > self.max_record_count:
            raise ValueError(f"`id_list` length exceeded max record count limit of this API endpoint, got list length {len(id_list)}, max record count limit: {self.max_record_count}")
        
        url = self.service_url + "/query"
        filter_field = 'OBJECTID'
        id_list_str = ', '.join([str(i) for i in id_list])

        params = {
            'f': 'json', 
            'where': '{} IN ({})'.format(filter_field, id_list_str),
            'returnIdsOnly': False, 
            'returnCountOnly': False, 
            'returnGeometry': True,
            'geometryType': 'esriGeometryMultipoint',
            'outFields': '*',  # The list of fields to be included in the returned result list
        }
        
        response = requests.post(
            url = url, 
            data = params,
        )
        return response.json()

    def __yield_chunk_of_id(self, list_of_ids:list):
        max_record_count = self.max_record_count
        for idx in range(0, len(list_of_ids), max_record_count):
            yield list_of_ids[idx : idx+max_record_count]
            
            
    def __get_data(self) -> dict:
        data_ids = self.__get_all_data_ids()
        grouped_data_ids = list(self.__yield_chunk_of_id(data_ids))

        logger.info(f"### Downloading {self.data_name} from {self.service_url}")
        
        for idx, id_group in enumerate(grouped_data_ids):
            logger.info(f"\tGroup {idx+1} of {len(grouped_data_ids)}")
            data_chunk = self.__download_data_by_id_list(id_group)

            if idx == 0: 
                result = data_chunk
            else:
                result['features'].extend(data_chunk["features"])

        return result

    def __get_metadata(self) -> str:
        url = f"https://www.arcgis.com/sharing/rest/content/items/{self.esri_id}/info/metadata/metadata.xml?format=default&output=html"
        # url = f"https://www.arcgis.com/sharing/rest/content/items/{self.esri_id}/info/metadata/metadata.xml?format=fgdc&output=html"
        response = requests.get(url)

        return response.text

    def download(self, data_path:Union[str, Path, None] = None):
        if data_path is None:
            data_path = Path("./esri_data_download")
        
        data_path = Path(data_path) / Path(self.data_name)

        if not data_path.exists():
            data_path.mkdir(parents=True)

        output_json_filename = self.data_name + ".json"
        output_properties_filename = self.data_name + "_ESRI_service_metadata.json"
        output_metadata_filename = "metadata.html"

        logger.info(f"\tWriting JSON file to {data_path}")
        with open(data_path / Path(output_json_filename), "w") as file_writer:
            result = self.__get_data()
            json.dump(result, file_writer)

        logger.info(f"\tWriting ESRI service properties metadata to {data_path}")
        with open(data_path / Path(output_properties_filename), "w") as file_writer:
            json.dump(self.service_prop, file_writer)

        logger.info(f"\tWriting metadata file to {data_path}")
        with open(data_path / Path(output_metadata_filename), "w") as file_writer:
            result = self.__get_metadata()
            file_writer.write(result)

        logger.info(f"\tFiles downloaded to {data_path}")
        
        return
        