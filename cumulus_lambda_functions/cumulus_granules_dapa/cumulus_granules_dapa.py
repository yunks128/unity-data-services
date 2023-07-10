import json
import os

from cumulus_lambda_functions.cumulus_wrapper.query_granules import GranulesQuery
from cumulus_lambda_functions.lib.authorization.uds_authorizer_abstract import UDSAuthorizorAbstract
from cumulus_lambda_functions.lib.authorization.uds_authorizer_factory import UDSAuthorizerFactory
from cumulus_lambda_functions.lib.json_validator import JsonValidator
from cumulus_lambda_functions.lib.lambda_logger_generator import LambdaLoggerGenerator
from cumulus_lambda_functions.lib.uds_db.db_constants import DBConstants
from cumulus_lambda_functions.lib.uds_db.uds_collections import UdsCollections
from cumulus_lambda_functions.lib.utils.lambda_api_gateway_utils import LambdaApiGatewayUtils

LOGGER = LambdaLoggerGenerator.get_logger(__name__, LambdaLoggerGenerator.get_level_from_env())


class CumulusGranulesDapa:
    def __init__(self, event):
        """
{'resource': '/collections/observation/items',
'path': '/collections/observation/items',
'httpMethod': 'GET',
'headers': {'Authorization': ' Bearer asdfafweaw'}, 'multiValueHeaders': {'Authorization': [' Bearer asdfafweaw']},
'queryStringParameters': {'datetime': 'asfa;lsfdjafal', 'bbox': '12,12,12,3', 'limit': '12'},
'multiValueQueryStringParameters': {'datetime': ['asfa;lsfdjafal'], 'bbox': ['12,12,12,3'], 'limit': ['12']}, 'pathParameters': None, 'stageVariables': None, 'requestContext': {'resourceId': 'hgdxj6', 'resourcePath': '/collections/observation/items', 'httpMethod': 'GET', 'extendedRequestId': 'SSEa6F80PHcF3xg=', 'requestTime': '17/May/2022:18:20:40 +0000', 'path': '/collections/observation/items', 'accountId': '884500545225', 'protocol': 'HTTP/1.1', 'stage': 'test-invoke-stage', 'domainPrefix': 'testPrefix', 'requestTimeEpoch': 1652811640832, 'requestId': '703f404d-cb95-43d3-8f48-523e5b1860e4', 'identity': {'cognitoIdentityPoolId': None, 'cognitoIdentityId': None, 'apiKey': 'test-invoke-api-key', 'principalOrgId': None, 'cognitoAuthenticationType': None, 'userArn': 'arn:aws:sts::884500545225:assumed-role/power_user/wai.phyo@jpl.nasa.gov', 'apiKeyId': 'test-invoke-api-key-id', 'userAgent': 'aws-internal/3 aws-sdk-java/1.12.201 Linux/5.4.181-109.354.amzn2int.x86_64 OpenJDK_64-Bit_Server_VM/25.322-b06 java/1.8.0_322 vendor/Oracle_Corporation cfg/retry-mode/standard', 'accountId': '884500545225', 'caller': 'AROAJZL4DI6MUSHCBBHGM:wai.phyo@jpl.nasa.gov', 'sourceIp': 'test-invoke-source-ip', 'accessKey': 'ASIA434CXH3EV56T6AS5', 'cognitoAuthenticationProvider': None, 'user': 'AROAJZL4DI6MUSHCBBHGM:wai.phyo@jpl.nasa.gov'}, 'domainName': 'testPrefix.testDomainName', 'apiId': 'gwaxi7ijl4'}, 'body': None, 'isBase64Encoded': False}

        :param event:
        """
        LOGGER.info(f'event: {event}')
        required_env = ['CUMULUS_BASE', 'CUMULUS_LAMBDA_PREFIX', 'ES_URL']
        if not all([k in os.environ for k in required_env]):
            raise EnvironmentError(f'one or more missing env: {required_env}')


        self.__event = event
        self.__jwt_token = ''
        self.__datetime = None
        self.__limit = 10
        self.__offset = 0
        self.__assign_values()
        self.__page_number = (self.__offset // self.__limit) + 1

        self.__cumulus_base = os.getenv('CUMULUS_BASE')
        self.__cumulus_lambda_prefix = os.getenv('CUMULUS_LAMBDA_PREFIX')

        self.__cumulus = GranulesQuery(self.__cumulus_base, self.__jwt_token)
        self.__cumulus.with_limit(self.__limit)
        self.__cumulus.with_page_number(self.__page_number)
        self.__get_time_range()
        self.__get_collection_id()
        self.__get_filter()
        self.__lambda_utils = LambdaApiGatewayUtils(self.__event, self.__limit)
        self.__authorizer: UDSAuthorizorAbstract = UDSAuthorizerFactory()\
            .get_instance(UDSAuthorizerFactory.cognito,
                          es_url=os.getenv('ES_URL'),
                          es_port=int(os.getenv('ES_PORT', '443'))
                          )

    def __get_filter(self):
        """
        https://portal.ogc.org/files/96288#rc_filter

        { "eq": [ { "property": "city" }, "Toronto" ] }

        {
          "like": [
            { "property": "name" },
            "Smith."
          ],
          "singleChar": ".",
          "nocase": true
        }

        {
  "in": {
     "value": { "property": "cityName" },
     "list": [ "Toronto", "Franfurt", "Tokyo", "New York" ],
     "nocase": false
  }
}
        :return:
        """
        if 'filter' not in self.__event:
            return self
        filter_event = json.loads(self.__event['filter'])
        if 'in' not in filter_event:
            return self
        schema = {
            "type": {
                "required": ["in"],
                "properties": {
                    "in": {
                        "type": "object",
                        "required": ["value", "list"],
                        "properties": {
                            "value": {
                                "type": "object",
                                "required": ["property"],
                                "properties": {
                                    "property": {"type": "string"}
                                }
                            },
                            "list": {
                                "type": "array",
                                "minItems": 1,
                                "items": {
                                    "type": "string"
                                }
                            }
                        }
                    }
                }
            }
        }
        filter_event_validator_result = JsonValidator(schema).validate(filter_event)
        if filter_event_validator_result is not None:
            LOGGER.error(f'invalid event: {filter_event_validator_result}. event: {filter_event}')
            return self
        search_key = filter_event['in']['value']['property']
        search_values = filter_event['in']['value']['list']
        self.__cumulus.with_filter(search_key, search_values)
        return self

    def __get_collection_id(self):
        if 'pathParameters' not in self.__event:
            return self
        path_param_dict = self.__event['pathParameters']
        if 'collectionId' not in path_param_dict:
            return self
        collection_id = path_param_dict['collectionId']
        if collection_id == '*':
            return self
        self.__cumulus.with_collection_id(path_param_dict['collectionId'])
        return self

    def __get_time_range(self):
        if self.__datetime is None:
            return
        if '/' not in self.__datetime:
            self.__cumulus.with_time(self.__datetime)
            return
        split_time_range = [k.strip() for k in self.__datetime.split('/')]
        if split_time_range[0] == '..':
            self.__cumulus.with_time_to(split_time_range[1])
            return
        if split_time_range[1] == '..':
            self.__cumulus.with_time_from(split_time_range[0])
            return
        self.__cumulus.with_time_range(split_time_range[0], split_time_range[1])
        return self

    def __assign_values(self):
        # commenting out checking Bearer token in the event as we are bypassing it in DAPA.
        # if 'headers' not in self.__event or 'Authorization' not in self.__event['headers']:
        #     raise ValueError('missing Authorization in HTTP headers')
        # self.__jwt_token = self.__event['headers']['Authorization']
        # if self.__jwt_token[:6].lower() == 'bearer':
        #     self.__jwt_token = self.__jwt_token[6:].strip()
        self.__jwt_token = 'NA'
        if 'queryStringParameters' not in self.__event or self.__event['queryStringParameters'] is None:
            return self
        query_str_dict = self.__event['queryStringParameters']
        if 'datetime' in query_str_dict:
            self.__datetime = query_str_dict['datetime']
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

    def start(self):
        try:
            if self.__collection_id == '':
                return {
                    'statusCode': 500,
                    'body': json.dumps({'message': 'unknown collection_id. require 1 collection id. '})
                }
            auth_info = self.__lambda_utils.get_authorization_info()
            collection_identifier = UdsCollections.decode_identifier(self.__collection_id)
            if not self.__authorizer.is_authorized_for_collection(DBConstants.create, self.__collection_id, auth_info['ldap_groups'],
                                                                  collection_identifier.tenant,
                                                                  collection_identifier.venue):
                LOGGER.debug(f'user: {auth_info["username"]} is not authorized for {self.__collection_id}')
                return {
                    'statusCode': 403,
                    'body': json.dumps({
                        'message': 'not authorized to create an action'
                    })
                }

            # TODO. cannot accept multiple collection_id. need single collection_id
            # get project and project_venue from collection_id and compare against authorization table
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
                    'type': 'FeatureCollection',  # TODO correct name?
                    'links': self.__get_pagination_urls(),
                    'features': cumulus_result['results']
                })
            }
        except Exception as e:
            LOGGER.exception(f'unexpected error')
            return {
                'statusCode': 500,
                'body': json.dumps({'message': f'unpredicted error: {str(e)}'})
            }
