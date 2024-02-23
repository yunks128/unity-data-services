import json
import os

from cumulus_lambda_functions.lib.lambda_logger_generator import LambdaLoggerGenerator

from cumulus_lambda_functions.cumulus_wrapper.query_collections import CollectionsQuery
LOGGER = LambdaLoggerGenerator.get_logger(__name__, LambdaLoggerGenerator.get_level_from_env())


class CollectionDapaQuery:
    max_limit = 50

    def __init__(self, collection_id, limit, offset, pagination_links, base_url):
        self.__base_url = base_url
        self.__pagination_links = pagination_links
        page_number = (offset // limit) + 1
        if 'CUMULUS_LAMBDA_PREFIX' not in os.environ:
            raise EnvironmentError('missing key: CUMULUS_LAMBDA_PREFIX')

        self.__cumulus_lambda_prefix = os.getenv('CUMULUS_LAMBDA_PREFIX')
        self.__cumulus = CollectionsQuery('https://na/dev', 'NA')
        self.__cumulus.with_limit(limit)
        LOGGER.debug(f'collection_id: {collection_id}')
        if collection_id is not None:
            if isinstance(collection_id, str):
                self.__cumulus.with_collection_id(collection_id)
            else:
                self.__cumulus.with_collections(collection_id)
        self.__cumulus.with_page_number(page_number)

    def __get_size(self):
        try:
            cumulus_size = self.__cumulus.get_size(self.__cumulus_lambda_prefix)
        except:
            LOGGER.exception(f'cannot get cumulus_size')
            cumulus_size = {'total_size': -1}
        return cumulus_size

    def start(self):
        try:
            cumulus_result = self.__cumulus.query_direct_to_private_api(self.__cumulus_lambda_prefix, self.__base_url)
            if 'server_error' in cumulus_result:
                return {
                    'statusCode': 500,
                    'body': {'message': cumulus_result['server_error']}
                }
            if 'client_error' in cumulus_result:
                return {
                    'statusCode': 400,
                    'body': {'message': cumulus_result['client_error']}
                }
            cumulus_size = self.__get_size()
            return {
                'statusCode': 200,
                'body': {
                    'numberMatched': cumulus_size['total_size'],
                    'numberReturned': len(cumulus_result['results']),
                    'stac_version': '1.0.0',
                    'type': 'FeatureCollection',
                    'links': self.__pagination_links,
                    'features': cumulus_result['results'],
                }
            }
        except Exception as e:
            LOGGER.exception(f'unexpected error')
            return {
                'statusCode': 500,
                'body': {'message': f'unpredicted error: {str(e)}'}
            }
