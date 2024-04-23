from cumulus_lambda_functions.cumulus_stac.item_transformer import ItemTransformer
from pystac import ItemCollection, Item

from cumulus_lambda_functions.lib.utils.file_utils import FileUtils

from cumulus_lambda_functions.lib.lambda_logger_generator import LambdaLoggerGenerator

from cumulus_lambda_functions.lib.aws.aws_s3 import AwsS3

LOGGER = LambdaLoggerGenerator.get_logger(__name__, LambdaLoggerGenerator.get_level_from_env())

"""
{
                'type': 'FeatureCollection', 
                'features': [{
                    'type': 'Feature', 
                    'stac_version': '1.0.0', 
                    'id': 'NEW_COLLECTION_EXAMPLE_L1B___9:test_file01',
                    'properties': {'start_datetime': '2016-01-31T18:00:00.009057Z',
                                'end_datetime': '2016-01-31T19:59:59.991043Z', 'created': '2016-02-01T02:45:59.639000Z',
                                'updated': '2022-03-23T15:48:21.578000Z', 'datetime': '1970-01-01T00:00:00Z'},
                    'geometry': {'type': 'Point', 'coordinates': [0.0, 0.0]}, 'links': [], 
                    'assets': {'data': {
                        'href': 's3://uds-test-cumulus-staging/NEW_COLLECTION_EXAMPLE_L1B___9/NEW_COLLECTION_EXAMPLE_L1B___9:test_file01/test_file01.nc',
                        'title': 'main data'}, 'metadata__cas': {
                        'href': 's3://uds-test-cumulus-staging/NEW_COLLECTION_EXAMPLE_L1B___9/NEW_COLLECTION_EXAMPLE_L1B___9:test_file01/test_file01.nc.cas',
                        'title': 'metadata cas'}, 'metadata__stac': {
                        'href': 's3://uds-test-cumulus-staging/NEW_COLLECTION_EXAMPLE_L1B___9/NEW_COLLECTION_EXAMPLE_L1B___9:test_file01/test_file01.nc.stac.json',
                        'title': 'metadata stac'}}, 
                    'bbox': [0.0, 0.0, 0.0, 0.0], 
                    'stac_extensions': [],
                    'collection': 'NEW_COLLECTION_EXAMPLE_L1B___9'}]}
                        

"""
class GranulesCnmIngesterLogic:
    def __init__(self):
        self.__s3 = AwsS3()
        self.__successful_features: ItemCollection = None
        self.__collection_id = None

    @property
    def collection_id(self):
        return self.__collection_id

    @collection_id.setter
    def collection_id(self, val):
        """
        :param val:
        :return: None
        """
        self.__collection_id = val
        return

    @property
    def successful_features(self):
        return self.__successful_features

    @successful_features.setter
    def successful_features(self, val):
        """
        :param val:
        :return: None
        """
        self.__successful_features = val
        return

    def load_successful_features_s3(self, successful_features_s3_url):
        self.__s3.set_s3_url(successful_features_s3_url)
        if not self.__s3.exists(self.__s3.target_bucket, self.__s3.target_key):
            LOGGER.error(f'missing successful_features: {successful_features_s3_url}')
            raise ValueError(f'missing successful_features: {successful_features_s3_url}')
        local_successful_features = self.__s3.download('/tmp')
        self.__successful_features = FileUtils.read_json(local_successful_features)
        FileUtils.remove_if_exists(local_successful_features)
        self.__successful_features = ItemCollection.from_dict(self.__successful_features)
        return

    def validate_granules(self):
        if self.successful_features is None:
            raise RuntimeError(f'NULL successful_features')
        missing_granules = []
        for each_granule in self.successful_features.items:
            missing_assets = []
            for each_asset_name, each_asset in each_granule.assets.items():
                temp_bucket, temp_key = self.__s3.split_s3_url(each_asset.href)
                if not self.__s3.exists(temp_bucket, temp_key):
                    missing_assets.append({each_asset_name: each_asset.href})
            if len(missing_assets) > 0:
                missing_granules.append({
                    'granule_id': each_granule.id,
                    'missing_assets': missing_assets
                })
        if len(missing_granules) > 0:
            LOGGER.error(f'missing_granules: {missing_granules}')
            raise ValueError(f'missing_granules: {missing_granules}')
        return

    def extract_collection_id(self):
        if self.successful_features is None:
            raise RuntimeError(f'NULL successful_features')
        if len(self.successful_features.items) < 1:
            LOGGER.error(f'not required to process. No Granules: {self.successful_features.to_dict(False)}')
            return
        sample_stac_metadata = None
        for each_asset_name, each_asset in self.successful_features.items[0].assets.items():
            if 'metadata' in each_asset.roles and each_asset.href.upper().endswith('JSON'):
                local_metadata_file = self.__s3.set_s3_url(each_asset.href).download('/tmp')
                stac_metadata = FileUtils.read_json(local_metadata_file)
                FileUtils.remove_if_exists(local_metadata_file)
                try:
                    sample_stac_metadata: Item = ItemTransformer().from_stac(stac_metadata)
                    break
                except:
                    LOGGER.debug(f'{each_asset_name} is not STAC metadata')
                    sample_stac_metadata = None
        if sample_stac_metadata is None:
            raise ValueError(f'missing STAC metadata. Unable to continue.')
        self.__collection_id = sample_stac_metadata.collection_id
        return

