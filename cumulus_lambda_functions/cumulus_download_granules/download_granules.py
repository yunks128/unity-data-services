import json
import logging
import os

import requests

from cumulus_lambda_functions.lib.aws.aws_s3 import AwsS3
from cumulus_lambda_functions.lib.utils.file_utils import FileUtils

LOGGER = logging.getLogger(__name__)


class DownloadGranules:
    DAPA_API_KEY = 'DAPA_API'
    UNITY_BEARER_TOKEN_KEY = 'UNITY_BEARER_TOKEN'
    COLLECTION_ID_KEY = 'COLLECTION_ID'
    DOWNLOAD_DIR_KEY = 'DOWNLOAD_DIR'

    LIMITS_KEY = 'LIMITS'
    DATE_FROM_KEY = 'DATE_FROM'
    DATE_TO_KEY = 'DATE_TO'
    VERIFY_SSL_KEY = 'VERIFY_SSL'

    def __init__(self):
        self.__dapa_api = ''
        self.__unity_bearer_token = ''
        self.__collection_id = ''
        self.__date_from = ''
        self.__date_to = ''
        self.__limit = 100
        self.__download_dir = '/tmp'
        self.__verify_ssl = True
        self.__s3 = AwsS3()

    def __set_props_from_env(self):
        missing_keys = [k for k in [self.DAPA_API_KEY, self.COLLECTION_ID_KEY, self.DOWNLOAD_DIR_KEY, self.UNITY_BEARER_TOKEN_KEY] if k not in os.environ]
        if len(missing_keys) > 0:
            raise ValueError(f'missing environment keys: {missing_keys}')

        self.__dapa_api = os.environ.get(self.DAPA_API_KEY)
        self.__unity_bearer_token = os.environ.get(self.UNITY_BEARER_TOKEN_KEY)
        self.__collection_id = os.environ.get(self.COLLECTION_ID_KEY)
        self.__download_dir = os.environ.get(self.DOWNLOAD_DIR_KEY)
        self.__download_dir = self.__download_dir[:-1] if self.__download_dir.endswith('/') else self.__download_dir
        if self.LIMITS_KEY not in os.environ:
            LOGGER.warning(f'missing {self.LIMITS_KEY}. using default: {self.__limit}')
        else:
            self.__limit = int(os.environ.get(self.LIMITS_KEY))

        self.__date_from = os.environ.get(self.DATE_FROM_KEY, '')
        self.__date_to = os.environ.get(self.DATE_TO_KEY, '')
        self.__verify_ssl = os.environ.get(self.VERIFY_SSL_KEY, 'TRUE').strip().upper() == 'TRUE'
        return self

    def __generate_dapa_url(self):
        self.__dapa_api = self.__dapa_api[:-1] if self.__dapa_api.endswith('/') else self.__dapa_api
        dapa_granules_api = f'{self.__dapa_api}/am-uds-dapa/collections/{self.__collection_id}/items?limit={self.__limit}&offset=0'
        if self.__date_from != '' or self.__date_to != '':
            dapa_granules_api = f"{dapa_granules_api}&datetime={self.__date_from if self.__date_from != '' else '..'}/{self.__date_to if self.__date_to != '' else '..'}"
        LOGGER.debug(f'dapa_granules_api: {dapa_granules_api}')
        return dapa_granules_api

    def __get_granules(self, dapa_granules_api):  # TODO pagination if needed
        LOGGER.debug(f'getting granules for: {dapa_granules_api}')
        header = {'Authorization': f'Bearer {self.__unity_bearer_token}'}
        response = requests.get(url=dapa_granules_api, headers=header, verify=self.__verify_ssl)
        if response.status_code > 400:
            raise RuntimeError(f'querying granules ends in error. status_code: {response.status_code}. url: {dapa_granules_api}. details: {response.text}')
        granules_result = json.loads(response.text)
        if 'features' not in granules_result:
            raise RuntimeError(f'missing features in response. invalid response: response: {granules_result}')
        return granules_result['features']

    def __get_downloading_urls(self, granules_result: list):
        if len(granules_result) < 1:
            LOGGER.warning(f'cannot find any granules')
            return []
        downloading_urls = [k['assets'] for k in granules_result]
        return downloading_urls

    def __download_one_granule(self, assets: dict):
        """
        sample assets
          {
            "data": {
              "href": "s3://am-uds-dev-cumulus-internal/ATMS_SCIENCE_Group___1/P1570515ATMSSCIENCEAAT16017044853900.PDS",
              "title": "P1570515ATMSSCIENCEAAT16017044853900.PDS",
              "description": "P1570515ATMSSCIENCEAAT16017044853900.PDS"
            },
            "metadata__data": {
              "href": "s3://am-uds-dev-cumulus-internal/ATMS_SCIENCE_Group___1/P1570515ATMSSCIENCEAAT16017044853901.PDS",
              "title": "P1570515ATMSSCIENCEAAT16017044853901.PDS",
              "description": "P1570515ATMSSCIENCEAAT16017044853901.PDS"
            },
            "metadata__xml": {
              "href": "s3://am-uds-dev-cumulus-internal/ATMS_SCIENCE_Group___1/P1570515ATMSSCIENCEAAT16017044853901.PDS.xml",
              "title": "P1570515ATMSSCIENCEAAT16017044853901.PDS.xml",
              "description": "P1570515ATMSSCIENCEAAT16017044853901.PDS.xml"
            },
            "metadata__cmr": {
              "href": "s3://am-uds-dev-cumulus-internal/ATMS_SCIENCE_Group___1/P1570515ATMSSCIENCEAAT16017044853900.PDS.cmr.xml",
              "title": "P1570515ATMSSCIENCEAAT16017044853900.PDS.cmr.xml",
              "description": "P1570515ATMSSCIENCEAAT16017044853900.PDS.cmr.xml"
            }
          }
        :param assets:
        :return:
        """
        error_log = []
        for k, v in assets.items():
            try:
                LOGGER.debug(f'downloading: {v["href"]}')
                self.__s3.set_s3_url(v['href']).download(self.__download_dir)
            except Exception as e:
                LOGGER.exception(f'failed to download {v}')
                v['cause'] = str(e)
                error_log.append(v)
        return error_log

    def start(self):
        self.__set_props_from_env()
        LOGGER.debug(f'creating download dir: {self.__download_dir}')
        FileUtils.mk_dir_p(self.__download_dir)
        dapa_granules_api = self.__generate_dapa_url()
        granules_result = self.__get_granules(dapa_granules_api)
        downloading_urls = self.__get_downloading_urls(granules_result)
        error_list = []
        for each in downloading_urls:
            LOGGER.debug(f'working on {each}')
            current_error_list = self.__download_one_granule(each)
            error_list.extend(current_error_list)
        if len(error_list) > 0:
            with open(f'{self.__download_dir}/error.log', 'w') as error_file:
                error_file.write(json.dumps(error_list, indent=4))
        return
