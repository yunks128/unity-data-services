import json
import os

import pystac

from cumulus_lambda_functions.cumulus_stac.collection_transformer import CollectionTransformer
from cumulus_lambda_functions.cumulus_wrapper.query_collections import CollectionsQuery
from cumulus_lambda_functions.lib.lambda_logger_generator import LambdaLoggerGenerator

LOGGER = LambdaLoggerGenerator.get_logger(__name__, LambdaLoggerGenerator.get_level_from_env())


class CumulusCreateCollectionDapa:
    def __init__(self, event):
        self.__event = event
        self.__request_body = None
        self.__cumulus_collection_query = CollectionsQuery('', '')
        self.__cumulus_lambda_prefix = os.getenv('CUMULUS_LAMBDA_PREFIX')

    def start(self):
        if 'body' not in self.__event:
            raise ValueError(f'missing body in {self.__event}')
        self.__request_body = json.loads(self.__event['body'])
        LOGGER.debug(f'request body: {self.__request_body}')
        validation_result = pystac.Collection.from_dict(self.__request_body).validate()
        if not isinstance(validation_result, list):
            LOGGER.error(f'request body is not valid STAC collection: {validation_result}')
            return {
                'statusCode': 500,
                'body': {'message': f'request body is not valid STAC Collection schema. check details',
                         'details': validation_result}
            }
        try:
            cumulus_collection_doc = CollectionTransformer().from_stac(self.__request_body)
            creation_result = self.__cumulus_collection_query.create_collection(cumulus_collection_doc, self.__cumulus_lambda_prefix)
            if 'status' not in creation_result:
                return {
                    'statusCode': 500,
                    'body': {
                        'message': {creation_result}
                    }
                }
        except Exception as e:
            LOGGER.exception('error while creating new collection in Cumulus')
            return {
                'statusCode': 500,
                'body': {
                    'message': f'error while creating new collection in Cumulus. check details',
                    'details': str(e)
                }
            }
        return {
            'statusCode': 200,
            'body': {
                'message': creation_result
            }
        }
