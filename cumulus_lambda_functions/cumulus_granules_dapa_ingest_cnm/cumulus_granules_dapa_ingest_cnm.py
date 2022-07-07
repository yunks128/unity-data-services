import json
import os

from cumulus_lambda_functions.cumulus_wrapper.query_granules import GranulesQuery
from cumulus_lambda_functions.lib.json_validator import JsonValidator
from cumulus_lambda_functions.lib.lambda_logger_generator import LambdaLoggerGenerator
from cumulus_lambda_functions.lib.time_utils import TimeUtils

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
        LOGGER.info(f'event: {event}')
        self.__event = event
        self.__request_body = {}

        self.__jwt_token = ''
        if 'CUMULUS_BASE' not in os.environ:
            raise EnvironmentError('missing key: CUMULUS_BASE')
        if 'CUMULUS_LAMBDA_PREFIX' not in os.environ:
            raise EnvironmentError('missing key: CUMULUS_LAMBDA_PREFIX')

        self.__cumulus_base = os.getenv('CUMULUS_BASE')
        self.__cumulus_lambda_prefix = os.getenv('CUMULUS_LAMBDA_PREFIX')

        self.__cumulus = GranulesQuery(self.__cumulus_base, self.__jwt_token)
        self.__cumulus.with_limit(self.__limit)
        self.__cumulus.with_page_number(self.__page_number)

    def __get_json_request_body(self):
        if 'requestContext' not in self.__event:
            raise ValueError(f'missing requestContext in {self.__event}')
        self.__request_body = self.__event['requestContext']
        validation_result = JsonValidator(REQUEST_BODY_SCHEMA).validate(self.__request_body)
        if validation_result is not None:
            raise ValueError(f'invalid cumulus granule json: {validation_result}')
        return self

    def start(self):
        """


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
        :return:
        """
        self.__get_json_request_body()
        error_list = []
        for each_granule in self.__request_body['features']:
            LOGGER.debug(f'executing: {each_granule}')
            try:
                sns_msg = {
                    'collection': each_granule['collection'],
                    'identifier': each_granule['id'],
                    'submissionTime': TimeUtils.get_current_time(),
                    "provider": self.__request_body['provider_id'],
                    "version": "1.6.0",  # TODO
                    'product': {
                        'name': each_granule['id'],
                        'dataVersion': '1',  # TODO
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
                cumulus_result = self.__cumulus.query_direct_to_private_api(self.__cumulus_lambda_prefix)
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
            'body': {'message': f'failed {len(error_list)}/{len(self.__request_body["features"])}', 'details': error_list}
        }
