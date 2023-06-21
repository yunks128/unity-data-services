import json

from pystac import ItemCollection, Item

from cumulus_lambda_functions.cumulus_stac.granules_catalog import GranulesCatalog
from cumulus_lambda_functions.stage_in_out.search_collections_factory import SearchCollectionsFactory
from cumulus_lambda_functions.stage_in_out.upload_granules_abstract import UploadGranulesAbstract
import logging
import os
import re
from cumulus_lambda_functions.lib.aws.aws_s3 import AwsS3

LOGGER = logging.getLogger(__name__)


class UploadGranulesByCompleteCatalogS3(UploadGranulesAbstract):
    CATALOG_FILE = 'CATALOG_FILE'
    COLLECTION_ID_KEY = 'COLLECTION_ID'
    STAGING_BUCKET_KEY = 'STAGING_BUCKET'

    VERIFY_SSL_KEY = 'VERIFY_SSL'
    DELETE_FILES_KEY = 'DELETE_FILES'

    def __init__(self) -> None:
        super().__init__()
        self.__gc = GranulesCatalog()
        self.__collection_id = ''
        self.__staging_bucket = ''
        self.__verify_ssl = True
        self.__delete_files = False
        self.__s3 = AwsS3()

    def __set_props_from_env(self):
        missing_keys = [k for k in [self.CATALOG_FILE, self.COLLECTION_ID_KEY, self.STAGING_BUCKET_KEY] if k not in os.environ]
        if len(missing_keys) > 0:
            raise ValueError(f'missing environment keys: {missing_keys}')

        self.__collection_id = os.environ.get(self.COLLECTION_ID_KEY)
        self.__staging_bucket = os.environ.get(self.STAGING_BUCKET_KEY)

        self.__verify_ssl = os.environ.get(self.VERIFY_SSL_KEY, 'TRUE').strip().upper() == 'TRUE'
        self.__delete_files = os.environ.get(self.DELETE_FILES_KEY, 'FALSE').strip().upper() == 'TRUE'
        return self

    def upload(self, **kwargs) -> str:
        self.__set_props_from_env()
        child_links = self.__gc.get_child_link_hrefs(os.environ.get(self.CATALOG_FILE))
        errors = []
        dapa_body_granules = []
        for each_child in child_links:
            try:
                current_granule_stac = self.__gc.get_granules_item(each_child)
                current_granules_dir = os.path.dirname(each_child)
                current_assets = self.__gc.extract_assets_href(current_granule_stac, current_granules_dir)
                if 'data' not in current_assets:
                    LOGGER.warning(f'skipping {each_child}. no data in {current_assets}')
                    continue
                current_granule_id = str(current_granule_stac.id)
                if current_granule_id in ['', 'NA', None]:
                    raise ValueError(f'invalid current_granule_id in granule {each_child}: {current_granule_id} ...')
                updating_assets = {}
                uploading_current_granule_stac = None
                for asset_type, asset_href in current_assets.items():
                    LOGGER.debug(f'uploading {asset_type}, {asset_href}')
                    s3_url = self.__s3.upload(asset_href, self.__staging_bucket, f'{self.__collection_id}:{current_granule_id}', self.__delete_files)
                    if asset_href == each_child:
                        uploading_current_granule_stac = s3_url
                    updating_assets[asset_type] = s3_url
                self.__gc.update_assets_href(current_granule_stac, updating_assets)
                current_granule_stac.id = current_granule_id
                current_granule_stac.collection_id = self.__collection_id
                if uploading_current_granule_stac is not None:  # upload metadata file again
                    self.__s3.set_s3_url(uploading_current_granule_stac)
                    self.__s3.upload_bytes(json.dumps(current_granule_stac.to_dict(False, False)).encode())
                current_granule_stac.id = f'{self.__collection_id}:{current_granule_id}'
                dapa_body_granules.append(current_granule_stac.to_dict(False, False))
            except Exception as e:
                LOGGER.exception(f'error while processing: {each_child}')
                errors.append({'href': each_child, 'error': str(e)})
        if len(errors) > 0:
            LOGGER.error(f'some errors while uploading granules: {errors}')
        LOGGER.debug(f'dapa_body_granules: {dapa_body_granules}')
        uploaded_item_collections = ItemCollection(items=dapa_body_granules)
        return json.dumps(uploaded_item_collections.to_dict(False))
