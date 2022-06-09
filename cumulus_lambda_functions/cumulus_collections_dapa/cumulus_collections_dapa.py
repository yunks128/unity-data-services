import os

from cumulus_lambda_functions.cumulus_wrapper.query_collections import CollectionsQuery
from cumulus_lambda_functions.lib.lambda_logger_generator import LambdaLoggerGenerator

LOGGER = LambdaLoggerGenerator.get_logger(__name__, LambdaLoggerGenerator.get_level_from_env())


class CumulusCollectionsDapa:
    def __init__(self, event):
        LOGGER.info(f'event: {event}')
        self.__event = event
        self.__jwt_token = 'NA'
        self.__limit = 10
        self.__offset = 0
        self.__page_number = (self.__offset // self.__limit) + 1
        if 'CUMULUS_BASE' not in os.environ:
            raise EnvironmentError('missing key: CUMULUS_BASE')
        if 'CUMULUS_LAMBDA_PREFIX' not in os.environ:
            raise EnvironmentError('missing key: CUMULUS_LAMBDA_PREFIX')

        self.__cumulus_base = os.getenv('CUMULUS_BASE')
        self.__cumulus_lambda_prefix = os.getenv('CUMULUS_LAMBDA_PREFIX')

        self.__cumulus = CollectionsQuery(self.__cumulus_base, self.__jwt_token)
        self.__cumulus.with_limit(self.__limit)
        self.__cumulus.with_page_number(self.__page_number)

    def __assign_values(self):
        if 'queryStringParameters' not in self.__event:
            return self
        query_str_dict = self.__event['queryStringParameters']
        if 'limit' in query_str_dict:
            self.__limit = int(query_str_dict['limit'])
        if 'offset' in query_str_dict:
            self.__offset = int(query_str_dict['offset'])
        return self

    def start(self):
        try:
            cumulus_result = self.__cumulus.query_direct_to_private_api(self.__cumulus_lambda_prefix)
        except Exception as e:
            return {
                'statusCode': 500,
                'body': {'message': f'unpredicted error: {str(e)}'}
            }
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
        return {
            'statusCode': 200,
            'body': json.dumps({'features': cumulus_result['results']})
        }
