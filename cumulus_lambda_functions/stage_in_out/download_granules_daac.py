import requests
from pystac import ItemCollection, Item, Asset

from cumulus_lambda_functions.lib.earthdata_login.urs_token_retriever import URSTokenRetriever
from cumulus_lambda_functions.lib.utils.file_utils import FileUtils
from cumulus_lambda_functions.stage_in_out.download_granules_abstract import DownloadGranulesAbstract
import json
import logging
import os


LOGGER = logging.getLogger(__name__)


class DownloadGranulesDAAC(DownloadGranulesAbstract):

    def __init__(self) -> None:
        super().__init__()
        self.__edl_token = None

    def __set_props_from_env(self):
        missing_keys = [k for k in [self.STAC_JSON, self.DOWNLOAD_DIR_KEY] if k not in os.environ]
        if len(missing_keys) > 0:
            raise ValueError(f'missing environment keys: {missing_keys}')
        self._retrieve_stac_json()
        self._setup_download_dir()
        self.__edl_token = URSTokenRetriever().start()
        return self

    def __download_one_granule_item(self, granule_item: Item):
        headers = {
            'Authorization': f'Bearer {self.__edl_token}'
        }
        new_asset_dict = {}
        for name, value_dict in granule_item.assets.items():
            if name not in self._downloading_keys:
                LOGGER.debug(f'skipping {name}. Not in downloading keys')
                continue
            value_dict: Asset = value_dict
            downloading_url = value_dict.href
            LOGGER.debug(f'downloading: {downloading_url}')
            r = requests.get(downloading_url, headers=headers)
            r.raise_for_status()
            local_file_path = os.path.join(self._download_dir, os.path.basename(downloading_url))
            with open(local_file_path, 'wb') as fd:
                fd.write(r.content)
            value_dict.href = os.path.join('.', os.path.basename(downloading_url))
            new_asset_dict[name] = value_dict
        granule_item.assets = new_asset_dict
        return granule_item

    def download(self, **kwargs) -> dict:
        self.__set_props_from_env()
        LOGGER.debug(f'creating download dir: {self._download_dir}')
        if len(self._granules_json.items) < 1:
            LOGGER.warning(f'cannot find any granules')
            return self._granules_json.to_dict(False)
        local_items = []
        error_list = []
        for each_item in self._granules_json.items:
            try:
                local_item = self.__download_one_granule_item(each_item)
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
        return granules_json_dict
