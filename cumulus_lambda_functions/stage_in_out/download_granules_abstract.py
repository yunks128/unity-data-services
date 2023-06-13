import json
import logging
import os
from abc import ABC, abstractmethod

from pystac import ItemCollection

from cumulus_lambda_functions.lib.utils.file_utils import FileUtils

LOGGER = logging.getLogger(__name__)


class DownloadGranulesAbstract(ABC):
    STAC_JSON = 'STAC_JSON'
    DOWNLOAD_DIR_KEY = 'DOWNLOAD_DIR'
    DOWNLOADING_KEYS = 'DOWNLOADING_KEYS'

    def __init__(self) -> None:
        super().__init__()
        self._granules_json: ItemCollection = {}
        self._download_dir = '/tmp'
        self._downloading_keys = set([k.strip() for k in os.environ.get(self.DOWNLOADING_KEYS, 'data').strip().split(',')])

    def _setup_download_dir(self):
        self._download_dir = os.environ.get(self.DOWNLOAD_DIR_KEY)
        self._download_dir = self._download_dir[:-1] if self._download_dir.endswith('/') else self._download_dir
        LOGGER.debug(f'creating download dir: {self._download_dir}')
        FileUtils.mk_dir_p(self._download_dir)
        return self
    
    def _retrieve_stac_json(self):
        raw_stac_json = os.environ.get(self.STAC_JSON)
        try:
            self._granules_json = ItemCollection.from_dict(json.loads(raw_stac_json))
            return self
        except:
            LOGGER.debug(f'raw_stac_json is not STAC_JSON: {raw_stac_json}. trying to see if file exists')
        if not FileUtils.file_exist(raw_stac_json):
            raise ValueError(f'missing file or not JSON: {raw_stac_json}')
        json_stac = FileUtils.read_json(raw_stac_json)
        if json_stac is None:
            raise ValueError(f'{raw_stac_json} is not JSON')
        self._granules_json = ItemCollection.from_dict(json_stac)
        return self
    
    @abstractmethod
    def download(self, **kwargs) -> dict:
        raise NotImplementedError()
