import json
import os

from cumulus_lambda_functions.cumulus_wrapper.query_granules import GranulesQuery
from cumulus_lambda_functions.lib.authorization.uds_authorizer_abstract import UDSAuthorizorAbstract
from cumulus_lambda_functions.lib.authorization.uds_authorizer_factory import UDSAuthorizerFactory
from cumulus_lambda_functions.lib.aws.aws_sns import AwsSns
from cumulus_lambda_functions.lib.json_validator import JsonValidator
from cumulus_lambda_functions.lib.lambda_logger_generator import LambdaLoggerGenerator
from cumulus_lambda_functions.lib.time_utils import TimeUtils
from cumulus_lambda_functions.lib.uds_db.db_constants import DBConstants
from cumulus_lambda_functions.lib.uds_db.uds_collections import UdsCollections
from cumulus_lambda_functions.lib.utils.lambda_api_gateway_utils import LambdaApiGatewayUtils

LOGGER = LambdaLoggerGenerator.get_logger(__name__, LambdaLoggerGenerator.get_level_from_env())

REQUEST_BODY_SCHEMA = {
    'type': 'object',
    'required': [
        'provider_id',
        'features',
    ],
    'properties': {
        'provider_id': {'type': 'string'},
        'features': {
            'type': 'array',
            'minItems': 1,
            'items': {
                'type': 'object',
                'required': ['collection', 'assets', 'id'],
                'properties': {
                    'id': {'type': 'string'},
                    'collection': {'type': 'string'},
                    'assets': {'type': 'object'},
                }
            }
        }
    }
}


class CumulusGranulesDapaIngestCnm:
    def __init__(self, event):
        """
{'resource': '/collections/observation/items',
'path': '/collections/observation/items',
'httpMethod': 'GET',
'headers': {'Authorization': ' Bearer asdfafweaw'}, 'multiValueHeaders': {'Authorization': [' Bearer asdfafweaw']},
'queryStringParameters': {'datetime': 'asfa;lsfdjafal', 'bbox': '12,12,12,3', 'limit': '12'}, 'multiValueQueryStringParameters': {'datetime': ['asfa;lsfdjafal'], 'bbox': ['12,12,12,3'], 'limit': ['12']}, 'pathParameters': None, 'stageVariables': None, 'requestContext': {'resourceId': 'hgdxj6', 'resourcePath': '/collections/observation/items', 'httpMethod': 'GET', 'extendedRequestId': 'SSEa6F80PHcF3xg=', 'requestTime': '17/May/2022:18:20:40 +0000', 'path': '/collections/observation/items', 'accountId': '884500545225', 'protocol': 'HTTP/1.1', 'stage': 'test-invoke-stage', 'domainPrefix': 'testPrefix', 'requestTimeEpoch': 1652811640832, 'requestId': '703f404d-cb95-43d3-8f48-523e5b1860e4', 'identity': {'cognitoIdentityPoolId': None, 'cognitoIdentityId': None, 'apiKey': 'test-invoke-api-key', 'principalOrgId': None, 'cognitoAuthenticationType': None, 'userArn': 'arn:aws:sts::884500545225:assumed-role/power_user/wai.phyo@jpl.nasa.gov', 'apiKeyId': 'test-invoke-api-key-id', 'userAgent': 'aws-internal/3 aws-sdk-java/1.12.201 Linux/5.4.181-109.354.amzn2int.x86_64 OpenJDK_64-Bit_Server_VM/25.322-b06 java/1.8.0_322 vendor/Oracle_Corporation cfg/retry-mode/standard', 'accountId': '884500545225', 'caller': 'AROAJZL4DI6MUSHCBBHGM:wai.phyo@jpl.nasa.gov', 'sourceIp': 'test-invoke-source-ip', 'accessKey': 'ASIA434CXH3EV56T6AS5', 'cognitoAuthenticationProvider': None, 'user': 'AROAJZL4DI6MUSHCBBHGM:wai.phyo@jpl.nasa.gov'}, 'domainName': 'testPrefix.testDomainName', 'apiId': 'gwaxi7ijl4'}, 'body': None, 'isBase64Encoded': False}

        :param event:
        """
        LOGGER.debug(f'event: {event}')
        required_env = ['SNS_TOPIC_ARN', 'COGNITO_UESR_POOL_ID', 'ES_URL']
        if not all([k in os.environ for k in required_env]):
            raise EnvironmentError(f'one or more missing env: {required_env}')
        self.__event = event
        self.__request_body = {}
        self.__sns_topic_arn = os.getenv('SNS_TOPIC_ARN')
        self.__lambda_utils = LambdaApiGatewayUtils(self.__event, 10)
        self.__authorizer: UDSAuthorizorAbstract = UDSAuthorizerFactory()\
            .get_instance(UDSAuthorizerFactory.cognito,
                          user_pool_id=os.getenv('COGNITO_UESR_POOL_ID'),
                          es_url=os.getenv('ES_URL'),
                          es_port=int(os.getenv('ES_PORT', '443'))
                          )

    def __get_json_request_body(self):
        if 'body' not in self.__event:
            raise ValueError(f'missing body in {self.__event}')
        self.__request_body = json.loads(self.__event['body'])
        validation_result = JsonValidator(REQUEST_BODY_SCHEMA).validate(self.__request_body)
        if validation_result is not None:
            LOGGER.debug(f'invalid request body: {self.__request_body}')
            raise ValueError(f'invalid cumulus granule json: {validation_result}')
        return self

    def start(self):
        """
        Publish granule messages to CNM SNS Topic.

Sample Output:
{
    "collection": "SNDR_SNPP_ATMS_L1A",
    "identifier": "SNDR.SNPP.ATMS.L1A.nominal2.01",
    "product": {
      "dataVersion": "1",
      "files": [
        {
          "name": "SNDR.SNPP.ATMS.L1A.nominal2.01.nc",
          "size": 9194361,
          "type": "data",
          "uri": "s3://am-uds-dev-cumulus-staging/SNDR_SNPP_ATMS_L1A/SNDR.SNPP.ATMS.L1A.nominal2.01.nc",
          "checksumType": "md5",
          "checksum": "2eafd390e5e7ac4b4d7d86d361fed50b"
        },
        {
          "name": "SNDR.SNPP.ATMS.L1A.nominal2.01.nc.cas",
          "size": 2673,
          "type": "metadata",
          "uri": "s3://am-uds-dev-cumulus-staging/SNDR_SNPP_ATMS_L1A/SNDR.SNPP.ATMS.L1A.nominal2.01.nc.cas",
          "checksumType": "md5",
          "checksum": "60e834d887ac6fb81ef63c4056c5b673"
        }
      ],
      "name": "SNDR.SNPP.ATMS.L1A.nominal2.01"
    },
    "provider": "SNPP",
    "submissionTime": "2022-07-06T00:00:00Z",
    "version": "1.6.0"
  }


Input to trigger CNM
Message:
{
 "collection": "SNDR_SNPP_ATMS_L1A",
 "identifier": "SNDR.SNPP.ATMS.L1A.nominal2.01",
 "product": {
  "dataVersion": "1",
  "files": [
   {
    "name": "SNDR.SNPP.ATMS.L1A.nominal2.01.nc",
    "size": 9194361,
    "type": "data",
    "uri": "s3://am-uds-dev-cumulus-staging/SNDR_SNPP_ATMS_L1A/SNDR.SNPP.ATMS.L1A.nominal2.01.nc",
    "checksumType": "md5",
    "checksum": "2eafd390e5e7ac4b4d7d86d361fed50b"
   },
   {
    "name": "SNDR.SNPP.ATMS.L1A.nominal2.01.nc.cas",
    "size": 2673,
    "type": "metadata",
    "uri": "s3://am-uds-dev-cumulus-staging/SNDR_SNPP_ATMS_L1A/SNDR.SNPP.ATMS.L1A.nominal2.01.nc.cas",
    "checksumType": "md5",
    "checksum": "60e834d887ac6fb81ef63c4056c5b673"
   }
  ],
  "name": "SNDR.SNPP.ATMS.L1A.nominal2.01"
 },
 "provider": "SNPP",
 "submissionTime": "2022-07-06T00:00:00Z",
 "version": "1.6.0"
}
aws sns publish --topic-arn arn:aws:sns:us-west-2:884500545225:am-uds-dev-cumulus-cnm-submission-sns --message file:///tmp/SNDR.SNPP.ATMS.L1A.nominal2.01.json



Test Input message
{
    "requestContext": {
        "provider_id": "SNPP",
        "features": [
            {
                "id": "SNDR.SNPP.ATMS.L1A.nominal2.01",
                "collection": "SNDR_SNPP_ATMS_L1A___1",
                "assets": {
                    "data": {
                        "href": "s3://am-uds-dev-cumulus-staging/SNDR_SNPP_ATMS_L1A/SNDR.SNPP.ATMS.L1A.nominal2.01.nc"
                    },
                    "metadata": {
                        "href": "s3://am-uds-dev-cumulus-staging/SNDR_SNPP_ATMS_L1A/SNDR.SNPP.ATMS.L1A.nominal2.01.nc.cas"
                    }
                }
            }
        ]
    }
}
        :return:
        """
        self.__get_json_request_body()
        collection_ids = list(set([k['collection'] for k in self.__request_body['features']]))
        if len(collection_ids) != 1:
            return {
                'statusCode': 500,
                'body': json.dumps({'message': f'does not allow multiple collections in a single request', 'details': collection_ids})
            }
        auth_info = self.__lambda_utils.get_authorization_info()
        collection_id = collection_ids[0]
        collection_identifier = UdsCollections.decode_identifier(collection_id)
        if not self.__authorizer.is_authorized_for_collection(DBConstants.create, collection_id, auth_info['ldap_groups'],
                                                              collection_identifier.tenant,
                                                              collection_identifier.venue):
            LOGGER.debug(f'user: {auth_info["username"]} is not authorized for {collection_id}')
            return {
                'statusCode': 403,
                'body': json.dumps({
                    'message': 'not authorized to create an action'
                })
            }
        error_list = []
        for each_granule in self.__request_body['features']:
            LOGGER.debug(f'executing: {each_granule}')
            try:
                collection_id_version = each_granule['collection'].split('___')
                sns_msg = {
                    'collection': collection_id_version[0],
                    'identifier': each_granule['id'],
                    'submissionTime': TimeUtils.get_current_time(),
                    "provider": self.__request_body['provider_id'],
                    "version": '1.6.0',  # TODO
                    'product': {
                        'name': each_granule['id'],
                        'dataVersion': collection_id_version[1],
                        'files': [{
                            'name': os.path.basename(v['href']),
                            'type': k,
                            'uri': v['href'],
                            'checksumType': 'md5',
                            'checksum': 'unknown',  # TODO
                            'size': -1,  # TODO
                        } for k, v in each_granule['assets'].items()],
                    }
                }
                LOGGER.debug(f'sending sns message: {sns_msg}')
                sns_response = AwsSns().set_topic_arn(self.__sns_topic_arn).publish_message(json.dumps(sns_msg))
                LOGGER.debug(f'published message result: {sns_response}')
            except Exception as e:
                LOGGER.exception(f'error while sending SNS msg for granule: {each_granule}')
                error_list.append({'message': str(e), 'feature': each_granule})
        if len(error_list) < 1:
            return {
                'statusCode': 200,
                'body': 'registered'
            }
        return {
            'statusCode': 500,
            'body': json.dumps({'message': f'failed {len(error_list)}/{len(self.__request_body["features"])}', 'details': error_list})
        }
