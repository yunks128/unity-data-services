import json
import os
from typing import Union

from cumulus_lambda_functions.lib.aws.aws_sns import AwsSns

from cumulus_lambda_functions.lib.lambda_logger_generator import LambdaLoggerGenerator
from pydantic import BaseModel
from cumulus_lambda_functions.lib.time_utils import TimeUtils

LOGGER = LambdaLoggerGenerator.get_logger(__name__, LambdaLoggerGenerator.get_level_from_env())


class SingleFeatureRequestBody(BaseModel):
    id: str
    collection: str
    assets: dict


class CnmRequestBody(BaseModel):
    provider_id: str
    features: list[SingleFeatureRequestBody]


class CollectionsDapaCnm:
    def __init__(self, request_body):
        if 'SNS_TOPIC_ARN' not in os.environ:
            raise EnvironmentError('missing key: SNS_TOPIC_ARN')
        self.__sns_topic_arn = os.getenv('SNS_TOPIC_ARN')
        self.__request_body = request_body

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
            'body': {'message': f'failed {len(error_list)}/{len(self.__request_body["features"])}',
                                'details': error_list}
        }
