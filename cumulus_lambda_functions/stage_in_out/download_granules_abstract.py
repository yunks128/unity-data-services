import json
import logging
import os
from abc import ABC, abstractmethod

from pystac import ItemCollection, Asset, Item

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

    def _set_props_from_env(self):
        raise NotImplementedError(f'to be implemented in concrete classes')

    def _download_one_item(self, downloading_url):
        raise NotImplementedError(f'to be implemented in concrete classes')

    def _download_one_granule_item(self, granule_item: Item):
        new_asset_dict = {}
        for name, value_dict in granule_item.assets.items():
            if name not in self._downloading_keys:
                LOGGER.debug(f'skipping {name}. Not in downloading keys')
                continue
            value_dict: Asset = value_dict
            downloading_url = value_dict.href
            LOGGER.debug(f'downloading: {downloading_url}')
            self._download_one_item(downloading_url)
            value_dict.href = os.path.join('.', os.path.basename(downloading_url))
            new_asset_dict[name] = value_dict
        granule_item.assets = new_asset_dict
        return granule_item

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
    
    def download(self, **kwargs) -> str:
        self._set_props_from_env()
        LOGGER.debug(f'creating download dir: {self._download_dir}')
        if len(self._granules_json.items) < 1:
            LOGGER.warning(f'cannot find any granules')
            return self._granules_json.to_dict(False)
        local_items = []
        error_list = []
        for each_item in self._granules_json.items:
            try:
                local_item = self._download_one_granule_item(each_item)
                local_items.append(local_item)
            except Exception as e:
                LOGGER.exception(f'error downloading granule: {each_item.id}')
                error_list.append({'error': str(e), 'id': each_item.id, })
        LOGGER.debug(f'finished downloading all granules')
        self._granules_json.items = local_items
        LOGGER.debug(f'writing features collection json to downloading directory')
        granules_json_dict = self._granules_json.to_dict(False)
        FileUtils.write_json(os.path.join(self._download_dir, 'downloaded_feature_collection.json'), granules_json_dict, overwrite=True, prettify=True)
        LOGGER.debug(f'writing errors if any')
        if len(error_list) > 0:
            with open(f'{self._download_dir}/error.log', 'w') as error_file:
                error_file.write(json.dumps(error_list, indent=4))
        return json.dumps(granules_json_dict)
