import json
import logging
import os
from abc import ABC, abstractmethod

from cumulus_lambda_functions.lib.utils.file_utils import FileUtils

LOGGER = logging.getLogger(__name__)


class DownloadGranulesAbstract(ABC):
    STAC_JSON = 'STAC_JSON'
    DOWNLOAD_DIR_KEY = 'DOWNLOAD_DIR'

    def __init__(self) -> None:
        super().__init__()
        self._granules_json = []
        self._download_dir = '/tmp'

    def _setup_download_dir(self):
        self._download_dir = os.environ.get(self.DOWNLOAD_DIR_KEY)
        self._download_dir = self._download_dir[:-1] if self._download_dir.endswith('/') else self._download_dir
        LOGGER.debug(f'creating download dir: {self._download_dir}')
        FileUtils.mk_dir_p(self._download_dir)
        return self
    
    def _retrieve_stac_json(self):
        raw_stac_json = os.environ.get(self.STAC_JSON)
        try:
            self._granules_json = json.loads(raw_stac_json)
            return self
        except:
            LOGGER.debug(f'raw_stac_json is not STAC_JSON: {raw_stac_json}. trying to see if file exists')
        if not FileUtils.file_exist(raw_stac_json):
            raise ValueError(f'missing file or not JSON: {raw_stac_json}')
        self._granules_json = FileUtils.read_json(raw_stac_json)
        if self._granules_json is None:
            raise ValueError(f'{raw_stac_json} is not JSON')
        return self
    
    @abstractmethod
    def download(self, **kwargs) -> list:
        raise NotImplementedError()
