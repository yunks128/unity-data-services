import json

from cumulus_lambda_functions.cumulus_stac.granules_catalog import GranulesCatalog
from cumulus_lambda_functions.stage_in_out.search_collections_factory import SearchCollectionsFactory
from cumulus_lambda_functions.stage_in_out.stage_in_out_utils import StageInOutUtils
from cumulus_lambda_functions.stage_in_out.upload_granules_abstract import UploadGranulesAbstract
import logging
import os
import re
from cumulus_lambda_functions.lib.aws.aws_s3 import AwsS3

LOGGER = logging.getLogger(__name__)


class UploadGranulesByCatalogS3(UploadGranulesAbstract):
    CATALOG_FILE = 'CATALOG_FILE'
    COLLECTION_ID_KEY = 'COLLECTION_ID'
    STAGING_BUCKET_KEY = 'STAGING_BUCKET'
    GRANULES_SEARCH_DOMAIN = 'GRANULES_SEARCH_DOMAIN'

    VERIFY_SSL_KEY = 'VERIFY_SSL'
    DELETE_FILES_KEY = 'DELETE_FILES'

    def __init__(self) -> None:
        super().__init__()
        self.__gc = GranulesCatalog()
        self.__collection_id = ''
        self.__collection_details = {}
        self.__staging_bucket = ''
        self.__verify_ssl = True
        self.__delete_files = False
        self.__s3 = AwsS3()

    def __set_props_from_env(self):
        missing_keys = [k for k in [self.CATALOG_FILE, self.COLLECTION_ID_KEY, self.GRANULES_SEARCH_DOMAIN, self.STAGING_BUCKET_KEY] if k not in os.environ]
        if len(missing_keys) > 0:
            raise ValueError(f'missing environment keys: {missing_keys}')

        self.__collection_id = os.environ.get(self.COLLECTION_ID_KEY)
        self.__staging_bucket = os.environ.get(self.STAGING_BUCKET_KEY)

        self.__verify_ssl = os.environ.get(self.VERIFY_SSL_KEY, 'TRUE').strip().upper() == 'TRUE'
        self.__delete_files = os.environ.get(self.DELETE_FILES_KEY, 'FALSE').strip().upper() == 'TRUE'
        return self

    def upload(self, **kwargs) -> list:
        self.__set_props_from_env()
        self.__collection_details = SearchCollectionsFactory().get_class(os.getenv('GRANULES_SEARCH_DOMAIN', 'MISSING_GRANULES_SEARCH_DOMAIN')).search()
        self.__collection_details = json.loads(self.__collection_details)

        granule_id_extraction = self.__collection_details['summaries']['granuleIdExtraction'][0]
        child_links = self.__gc.get_child_link_hrefs(os.environ.get(self.CATALOG_FILE))
        errors = []
        dapa_body_granules = []
        for each_child in child_links:
            try:
                current_granule_stac = self.__gc.get_granules_item(each_child)
                current_assets = self.__gc.extract_assets_href(current_granule_stac)
                if 'data' not in current_assets:
                    LOGGER.warning(f'skipping {each_child}. no data in {current_assets}')
                    continue

                current_granule_id = re.findall(granule_id_extraction, os.path.basename(current_assets['data']))
                if len(current_granule_id) < 1:
                    LOGGER.warning(f'skipping {each_child}. cannot be matched to granule_id: {current_granule_id}')
                    continue
                current_granule_id = current_granule_id[0]

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
                dapa_body_granules.append({
                    'id': f'{self.__collection_id}:{current_granule_id}',
                    'collection': self.__collection_id,
                    'assets': {k: v.to_dict() for k, v in current_granule_stac.assets.items()},
                })
            except Exception as e:
                LOGGER.exception(f'error while processing: {each_child}')
                errors.append({'href': each_child, 'error': str(e)})

        if len(errors) > 0:
            LOGGER.error(f'some errors while uploading granules: {errors}')
        LOGGER.debug(f'dapa_body_granules: {dapa_body_granules}')
        StageInOutUtils.write_output_to_file(dapa_body_granules)
        return dapa_body_granules
