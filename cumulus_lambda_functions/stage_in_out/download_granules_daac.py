import requests

from cumulus_lambda_functions.lib.earthdata_login.urs_token_retriever import URSTokenRetriever
from cumulus_lambda_functions.stage_in_out.download_granules_abstract import DownloadGranulesAbstract
import json
import logging
import os

from cumulus_lambda_functions.lib.aws.aws_s3 import AwsS3
from cumulus_lambda_functions.lib.utils.file_utils import FileUtils

LOGGER = logging.getLogger(__name__)


class DownloadGranulesDAAC(DownloadGranulesAbstract):
    DOWNLOAD_DIR_KEY = 'DOWNLOAD_DIR'
    STAC_JSON = 'STAC_JSON'

    def __init__(self) -> None:
        super().__init__()
        self.__download_dir = '/tmp'
        self.__s3 = AwsS3()
        self.__granules_json = []
        self.__edl_token = None

    def __set_props_from_env(self):
        missing_keys = [k for k in [self.STAC_JSON, self.DOWNLOAD_DIR_KEY] if k not in os.environ]
        if len(missing_keys) > 0:
            raise ValueError(f'missing environment keys: {missing_keys}')
        self.__granules_json = json.loads(os.environ.get(self.STAC_JSON))
        self.__download_dir = os.environ.get(self.DOWNLOAD_DIR_KEY)
        self.__download_dir = self.__download_dir[:-1] if self.__download_dir.endswith('/') else self.__download_dir
        self.__edl_token = URSTokenRetriever().start()
        return self

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
        headers = {
            'Authorization': f'Bearer ${self.__edl_token}'
        }
        for k, v in assets.items():
            try:
                LOGGER.debug(f'downloading: {v["href"]}')
                print(v["href"])
                r = requests.get(v['href'], headers=headers, allow_redirects=True)
                with open(os.path.join(self.__download_dir, os.path.basename(v["href"])), 'wb') as fd:
                    fd.write(r.content)
            except Exception as e:
                LOGGER.exception(f'failed to download {v}')
                v['cause'] = str(e)
                error_log.append(v)
        return error_log

    def download(self, **kwargs) -> list:
        self.__set_props_from_env()
        LOGGER.debug(f'creating download dir: {self.__download_dir}')
        FileUtils.mk_dir_p(self.__download_dir)
        downloading_urls = self.__get_downloading_urls(self.__granules_json)
        error_list = []
        for each in downloading_urls:
            LOGGER.debug(f'working on {each}')
            current_error_list = self.__download_one_granule(each)
            error_list.extend(current_error_list)
        if len(error_list) > 0:
            with open(f'{self.__download_dir}/error.log', 'w') as error_file:
                error_file.write(json.dumps(error_list, indent=4))
        return downloading_urls
