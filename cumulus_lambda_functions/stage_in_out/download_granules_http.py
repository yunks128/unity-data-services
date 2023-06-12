import shutil

import requests

from cumulus_lambda_functions.stage_in_out.download_granules_abstract import DownloadGranulesAbstract
import json
import logging
import os

LOGGER = logging.getLogger(__name__)


class DownloadGranulesHttp(DownloadGranulesAbstract):

    def __init__(self) -> None:
        super().__init__()

    def __set_props_from_env(self):
        missing_keys = [k for k in [self.STAC_JSON, self.DOWNLOAD_DIR_KEY] if k not in os.environ]
        if len(missing_keys) > 0:
            raise ValueError(f'missing environment keys: {missing_keys}')
        self._retrieve_stac_json()
        self._setup_download_dir()
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
        local_item = {}
        for k, v in assets.items():
            local_item[k] = v
            try:
                LOGGER.debug(f'downloading: {v["href"]}')
                downloading_response = requests.get(v['href'])
                downloading_response.raise_for_status()
                downloading_response.raw.decode_content = True
                local_file_path = os.path.join(self._download_dir, os.path.basename(v["href"]))
                with open(local_file_path, 'wb') as f:
                    shutil.copyfileobj(downloading_response.raw, f)
                local_item[k]['href'] = local_file_path
            except Exception as e:
                LOGGER.exception(f'failed to download {v}')
                local_item[k]['description'] = f'download failed. {str(e)}'
                error_log.append(v)
        return local_item, error_log

    def download(self, **kwargs) -> list:
        self.__set_props_from_env()
        downloading_urls = self.__get_downloading_urls(self._granules_json)
        error_list = []
        local_items = []
        for each in downloading_urls:
            LOGGER.debug(f'working on {each}')
            local_item, current_error_list = self.__download_one_granule(each)
            local_items.append({'assets': local_item})
            error_list.extend(current_error_list)
        if len(error_list) > 0:
            with open(f'{self._download_dir}/error.log', 'w') as error_file:
                error_file.write(json.dumps(error_list, indent=4))
        return local_items
