import json
import logging
import os
import re
from collections import defaultdict
from glob import glob

import requests

from cumulus_lambda_functions.lib.aws.aws_s3 import AwsS3

LOGGER = logging.getLogger(__name__)


class UploadGranules:
    DAPA_API_KEY = 'DAPA_API'
    UNITY_BEARER_TOKEN_KEY = 'UNITY_BEARER_TOKEN'
    COLLECTION_ID_KEY = 'COLLECTION_ID'
    PROVIDER_ID_KEY = 'PROVIDER_ID'
    UPLOAD_DIR_KEY = 'UPLOAD_DIR'
    STAGING_BUCKET_KEY = 'STAGING_BUCKET'

    VERIFY_SSL_KEY = 'VERIFY_SSL'
    DELETE_FILES_KEY = 'DELETE_FILES'

    def __init__(self):
        self.__dapa_api = ''
        self.__unity_bearer_token = ''
        self.__collection_id = ''
        self.__collection_details = {}
        self.__uploading_granules = []
        self.__provider_id = ''
        self.__staging_bucket = ''
        self.__upload_dir = '/tmp'
        self.__verify_ssl = True
        self.__delete_files = False
        self.__s3 = AwsS3()
        self.__raw_files = []

    def __set_props_from_env(self):
        missing_keys = [k for k in [self.DAPA_API_KEY, self.COLLECTION_ID_KEY, self.PROVIDER_ID_KEY, self.UPLOAD_DIR_KEY, self.UNITY_BEARER_TOKEN_KEY, self.STAGING_BUCKET_KEY] if k not in os.environ]
        if len(missing_keys) > 0:
            raise ValueError(f'missing environment keys: {missing_keys}')

        self.__dapa_api = os.environ.get(self.DAPA_API_KEY)
        self.__dapa_api = self.__dapa_api[:-1] if self.__dapa_api.endswith('/') else self.__dapa_api
        self.__unity_bearer_token = os.environ.get(self.UNITY_BEARER_TOKEN_KEY)
        self.__collection_id = os.environ.get(self.COLLECTION_ID_KEY)
        self.__provider_id = os.environ.get(self.PROVIDER_ID_KEY)
        self.__staging_bucket = os.environ.get(self.STAGING_BUCKET_KEY)

        self.__upload_dir = os.environ.get(self.UPLOAD_DIR_KEY)
        self.__upload_dir = self.__upload_dir[:-1] if self.__upload_dir.endswith('/') else self.__upload_dir

        self.__verify_ssl = os.environ.get(self.VERIFY_SSL_KEY, 'TRUE').strip().upper() == 'TRUE'
        self.__delete_files = os.environ.get(self.DELETE_FILES_KEY, 'FALSE').strip().upper() == 'TRUE'
        return self

    def __get_collection_stac(self):
        LOGGER.debug(f'getting collection details for: {self.__collection_id}')
        header = {'Authorization': f'Bearer {self.__unity_bearer_token}'}
        dapa_collection_url = f'{self.__dapa_api}/collections?limit=1000'
        # TODO need better endpoint to get exactly 1 collection
        # TODO pagination?
        response = requests.get(url=dapa_collection_url, headers=header, verify=self.__verify_ssl)
        if response.status_code > 400:
            raise RuntimeError(f'querying granules ends in error. status_code: {response.status_code}. url: {dapa_collection_url}. details: {response.text}')
        collections_result = json.loads(response.text)
        if 'features' not in collections_result:
            raise RuntimeError(f'missing features in response. invalid response: response: {collections_result}')
        print(self.__collection_id)
        collection_details = [each_collection for each_collection in collections_result['features'] if self.__collection_id == each_collection["id"]]
        if len(collection_details) < 1:
            raise RuntimeError(f'unable to find collection in DAPA')
        self.__collection_details = collection_details[0]
        return self

    def __sort_granules(self):
        file_regex_list = {k['type']: k['href'].split('___')[-1] for k in self.__collection_details['links'] if not k['title'].endswith('cmr.xml')}
        granule_id_extraction = self.__collection_details['summaries']['granuleIdExtraction']
        granules = defaultdict(dict)
        for each_file in self.__raw_files:
            each_filename = os.path.basename(each_file)
            each_granule_id = re.findall(granule_id_extraction, each_filename)
            if len(each_granule_id) < 1:
                LOGGER.warning(f'skipping file that cannot be matched to granule_id: {each_file}')
                continue
            each_granule_id = each_granule_id[0]
            if isinstance(each_granule_id, tuple):
                each_granule_id = each_granule_id[0]
            data_type = [k for k, v in file_regex_list.items() if len(re.findall(v, each_filename)) > 0]
            if len(data_type) != 1:
                LOGGER.warning(f'skipping file that cannot be matched to a datatype: {each_file}.. data_type: {data_type}')
                continue
            data_type = data_type[0]
            if data_type in granules[each_granule_id]:
                LOGGER.warning(f'duplicated data type: {data_type}. file: {each_file}. existing data_type: {granules[each_granule_id][data_type]}')
                continue
            granules[each_granule_id][data_type] = {
                'href': each_file
            }
        LOGGER.debug(f'filtering granules w/o data. original len: {len(granules)} original granules: {granules}')
        granules = {k: v for k, v in granules.items() if 'data' in v}
        LOGGER.debug(f'filtered granules. original len: {len(granules)}. granules: {granules}')
        return granules

    def __upload_granules(self, granule_assets: dict, granule_id: str):
        for data_type, href_dict in granule_assets.items():
            LOGGER.debug(f'uploading {href_dict}')
            s3_url = self.__s3.upload(href_dict['href'], self.__staging_bucket, granule_id, self.__delete_files)
            href_dict['href'] = s3_url
        return self

    def __execute_dapa_cnm_ingestion(self, cnm_ingest_body: dict):
        dapa_ingest_cnm_api = f'{self.__dapa_api}/am-uds-dapa/collections/'
        LOGGER.debug(f'getting granules for: {dapa_ingest_cnm_api}')
        header = {
            'Authorization': f'Bearer {self.__unity_bearer_token}',
            'Content-Type': 'application/json',
        }
        response = requests.put(url=dapa_ingest_cnm_api, headers=header, verify=self.__verify_ssl, data=json.dumps(cnm_ingest_body))
        if response.status_code > 400:
            raise RuntimeError(
                f'querying granules ends in error. status_code: {response.status_code}. url: {dapa_ingest_cnm_api}. details: {response.text}')
        granules_result = response.text
        return granules_result

    def start(self):
        """

        1. recursively get all files from upload dir
        2. use collection id to get the links
        3. group files from step-1 into granules
        4. get granule ID ???
        5. upload to staging bucket with granuleID as key
        6. call DAPA endpoint to start the registration to cumulus
        :return:
        """
        self.__set_props_from_env()
        LOGGER.debug(f'listing files recursively in dir: {self.__upload_dir}')
        self.__raw_files = glob(f'{self.__upload_dir}/**/*', recursive=True)
        self.__get_collection_stac()
        on_disk_granules = self.__sort_granules()
        LOGGER.debug(f'on_disk_granules: {on_disk_granules}')
        dapa_body_granules = []
        for granule_id, granule_hrefs in on_disk_granules.items():
            self.__upload_granules(granule_hrefs, granule_id)
            dapa_body_granules.append({
                'id': granule_id,
                'collection': self.__collection_id,
                'assets': granule_hrefs,
            })
        LOGGER.debug(f'dapa_body_granules: {dapa_body_granules}')
        dapa_body = {
            "provider_id": self.__provider_id,
            "features": dapa_body_granules
        }
        LOGGER.debug(f'dapa_body_granules: {dapa_body}')
        dapa_ingest_reuslt = self.__execute_dapa_cnm_ingestion(dapa_body)
        return dapa_ingest_reuslt
