import json
import os

from cumulus_lambda_functions.cumulus_wrapper.query_collections import CollectionsQuery
from cumulus_lambda_functions.lib.authorization.uds_authorizer_abstract import UDSAuthorizorAbstract
from cumulus_lambda_functions.lib.authorization.uds_authorizer_factory import UDSAuthorizerFactory
from cumulus_lambda_functions.lib.lambda_logger_generator import LambdaLoggerGenerator
from cumulus_lambda_functions.lib.uds_db.db_constants import DBConstants
from cumulus_lambda_functions.lib.utils.lambda_api_gateway_utils import LambdaApiGatewayUtils

LOGGER = LambdaLoggerGenerator.get_logger(__name__, LambdaLoggerGenerator.get_level_from_env())


class CumulusCollectionsDapa:
    RESOURCE = 'COLLECTIONS'
    ACTION = 'READ'

    def __init__(self, event):
        LOGGER.info(f'event: {event}')
        self.__event = event
        self.__jwt_token = 'NA'
        self.__limit = 10
        self.__offset = 0
        self.__assign_values()
        self.__page_number = (self.__offset // self.__limit) + 1
        if 'COGNITO_UESR_POOL_ID' not in os.environ:
            raise EnvironmentError('missing key: COGNITO_UESR_POOL_ID')
        if 'CUMULUS_BASE' not in os.environ:
            raise EnvironmentError('missing key: CUMULUS_BASE')
        if 'CUMULUS_LAMBDA_PREFIX' not in os.environ:
            raise EnvironmentError('missing key: CUMULUS_LAMBDA_PREFIX')

        self.__cumulus_base = os.getenv('CUMULUS_BASE')
        self.__cumulus_lambda_prefix = os.getenv('CUMULUS_LAMBDA_PREFIX')

        self.__cumulus = CollectionsQuery(self.__cumulus_base, self.__jwt_token)
        self.__cumulus.with_limit(self.__limit)
        self.__cumulus.with_page_number(self.__page_number)
        self.__get_collection_id()
        self.__lambda_utils = LambdaApiGatewayUtils(self.__event, self.__limit)
        self.__authorizer: UDSAuthorizorAbstract = UDSAuthorizerFactory().get_instance(UDSAuthorizerFactory.cognito, user_pool_id=os.environ.get('COGNITO_UESR_POOL_ID'))

    def __get_collection_id(self):
        if 'pathParameters' not in self.__event:
            return self
        path_param_dict = self.__event['pathParameters']
        if path_param_dict is None or 'collectionId' not in path_param_dict:
            return self
        collection_id = path_param_dict['collectionId']
        if collection_id == '*':
            return self
        self.__cumulus.with_collection_id(path_param_dict['collectionId'])
        return self

    def __assign_values(self):
        if 'queryStringParameters' not in self.__event or self.__event['queryStringParameters'] is None:
            return self
        query_str_dict = self.__event['queryStringParameters']
        if 'limit' in query_str_dict:
            self.__limit = int(query_str_dict['limit'])
        if 'offset' in query_str_dict:
            self.__offset = int(query_str_dict['offset'])
        return self

    def __get_size(self):
        try:
            cumulus_size = self.__cumulus.get_size(self.__cumulus_lambda_prefix)
        except:
            LOGGER.exception(f'cannot get cumulus_size')
            cumulus_size = {'total_size': -1}
        return cumulus_size

    def __get_pagination_urls(self):
        try:
            pagination_links = self.__lambda_utils.generate_pagination_links()
        except Exception as e:
            LOGGER.exception(f'error while generating pagination links')
            return [{'message': f'error while generating pagination links: {str(e)}'}]
        return pagination_links

    def __setup_authorized_tenant_venue(self):
        username = self.__lambda_utils.get_authorization_info()['username']
        LOGGER.debug(f'query for user: {username}')
        authorized_tenants = self.__authorizer.get_authorized_tenant(username, self.ACTION, self.RESOURCE)
        for each_tenant in authorized_tenants:
            self.__cumulus.with_tenant(each_tenant[DBConstants.tenant], each_tenant[DBConstants.tenant_venue])
        return self

    def start(self):
        try:
            self.__setup_authorized_tenant_venue()
            cumulus_result = self.__cumulus.query_direct_to_private_api(self.__cumulus_lambda_prefix)
            if 'server_error' in cumulus_result:
                return {
                    'statusCode': 500,
                    'body': json.dumps({'message': cumulus_result['server_error']})
                }
            if 'client_error' in cumulus_result:
                return {
                    'statusCode': 400,
                    'body': json.dumps({'message': cumulus_result['client_error']})
                }
            cumulus_size = self.__get_size()
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'numberMatched': cumulus_size['total_size'],
                    'numberReturned': len(cumulus_result['results']),
                    'stac_version': '1.0.0',
                    'type': 'FeatureCollection',
                    'links': self.__get_pagination_urls(),
                    'features': cumulus_result['results'],
                })
            }
        except Exception as e:
            LOGGER.exception(f'unexpected error')
            return {
                'statusCode': 500,
                'body': json.dumps({'message': f'unpredicted error: {str(e)}'})
            }
