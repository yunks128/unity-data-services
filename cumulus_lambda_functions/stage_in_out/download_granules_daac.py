import time

import requests

from cumulus_lambda_functions.lib.earthdata_login.urs_token_retriever import URSTokenRetriever
from cumulus_lambda_functions.stage_in_out.download_granules_abstract import DownloadGranulesAbstract
import json
import logging
import os


LOGGER = logging.getLogger(__name__)


class DownloadGranulesDAAC(DownloadGranulesAbstract):

    def __init__(self) -> None:
        super().__init__()
        self.__edl_token = None

    def _set_props_from_env(self):
        missing_keys = [k for k in [self.STAC_JSON, self.DOWNLOAD_DIR_KEY] if k not in os.environ]
        if len(missing_keys) > 0:
            raise ValueError(f'missing environment keys: {missing_keys}')
        self._retrieve_stac_json()
        self._setup_download_dir()
        self.__edl_token = URSTokenRetriever().start()
        return self

    def _download_one_item(self, downloading_url):
        headers = {
            'Authorization': f'Bearer {self.__edl_token}'
        }
        r = requests.get(downloading_url, headers=headers)
        download_count = 1
        while r.status_code in [502, 404] and download_count < 5:
            time.sleep(30)
            r = requests.get(downloading_url, headers=headers)
            download_count += 1
        r.raise_for_status()
        local_file_path = os.path.join(self._download_dir, os.path.basename(downloading_url))
        with open(local_file_path, 'wb') as fd:
            fd.write(r.content)
        return local_file_path
