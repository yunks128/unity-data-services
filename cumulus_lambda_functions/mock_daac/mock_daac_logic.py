import json
import os
import random

from cumulus_lambda_functions.lib.aws.aws_s3 import AwsS3

from cumulus_lambda_functions.lib.lambda_logger_generator import LambdaLoggerGenerator

from cumulus_lambda_functions.lib.time_utils import TimeUtils

from cumulus_lambda_functions.lib.aws.aws_sns import AwsSns
LOGGER = LambdaLoggerGenerator.get_logger(__name__, LambdaLoggerGenerator.get_level_from_env())


class MockDaacLogic:
    NO_RESPONSE_PERC = 'NO_RESPONSE_PERC'
    NO_RESPONSE_PERC_DEFAULT = .25
    FAIL_PERC = 'FAIL_PERC'
    FAIL_PERC_DEFAULT = .25
    UDS_ARCHIVE_SNS_TOPIC_ARN = 'UDS_ARCHIVE_SNS_TOPIC_ARN'

    def __init__(self):
        self.__no_response_percentage = float(os.environ.get(self.NO_RESPONSE_PERC, self.NO_RESPONSE_PERC_DEFAULT))
        self.__fail_percentage = float(os.environ.get(self.NO_RESPONSE_PERC, self.NO_RESPONSE_PERC_DEFAULT))
        self.__fail_percentage += self.__no_response_percentage
        self.__sns_topic_arn = os.environ.get(self.UDS_ARCHIVE_SNS_TOPIC_ARN)
        self.__sns = AwsSns().set_topic_arn(self.__sns_topic_arn)
        self.__response_message = {}
        self.__s3 = AwsS3()

    def __send_random_result(self):
        # https://github.com/podaac/cloud-notification-message-schema?tab=readme-ov-file#response-message-fields
        random_success = random.uniform(0, 1)
        if random_success < self.__no_response_percentage:
            LOGGER.debug(f'intentionally not sending any message')
            return
        if random_success < self.__fail_percentage:
            LOGGER.debug(f'sending failure message')
            self.__response_message['response'] = {
                'status': 'FAILURE',
                'errorCode': ["VALIDATION_ERROR", "PROCESSING_ERROR", "TRANSFER_ERROR"][random.randint(0, 2)],
                'errorMessage': 'This is a sample failure message',
            }
            sns_response = self.__sns.publish_message(json.dumps(self.__response_message))
            LOGGER.debug(f'sns_response: {sns_response}')
            return
        self.__response_message['response'] = {
            'status': 'SUCCESS',
        }
        LOGGER.debug(f'sending success message')
        sns_response = self.__sns.publish_message(json.dumps(self.__response_message))
        LOGGER.debug(f'sns_response: {sns_response}')
        return

    def __check_s3_file(self, input_files: list):
        for each_file in input_files:
            s3_obj_size = self.__s3.set_s3_url(each_file['uri']).get_s3_obj_size()
        return


    def start(self, event):
        LOGGER.info(f'event: {event}')
        # Check input message is validated according to https://github.com/podaac/cloud-notification-message-schema?tab=readme-ov-file#notification-message-fields
        # Check if S3 can be downloaded
        # .25/.25/.50 P() on No send, send failure, send success
        # Return with this message: https://github.com/podaac/cloud-notification-message-schema?tab=readme-ov-file#response-message-fields
        validated_input_event = event
        example_incoming_msg = {
            "collection": "<DAAC collection name that user has created, and provided via configuration>",
            "identifier": "< Granule ID starting with Unity identifiers or simply a UUID? example: URN:NASA:UNITY:UDS_LOCAL_TEST:DEV:UDS_COLLECTION___2404251100:abcd.1234.efgh.test_file05>",
            "submissionTime": "<Lambda will generate this: 2024-05-16T16:04:44.776376>",
            "provider": "<Should be provided by user. But this is not mandatory. example: unity. NO. It will be tenant which is pulled from identifier>",
            "version": "<one of 1.0, 1.1, 1.2, 1.3>",
            "product": {
                "name": "<recommendation is Granule ID: URN:NASA:UNITY:UDS_LOCAL_TEST:DEV:UDS_COLLECTION___2404251100:abcd.1234.efgh.test_file05.. No. Everything after tenant/venue.>",
                "dataVersion": "<This is not mandatory. Should we include? Currently, it is simply the date. example: 2404251100. It has to be the DAAC version. User needs to provide it>",
                "files": [
                    {
                        "name": "abcd.1234.efgh.test_file05.data.stac.json",
                        "type": "data",
                        "uri": "s3://unity-dev-cumulus-unity-william-test-1/URN:NASA:UNITY:UDS_LOCAL_TEST:DEV:UDS_COLLECTION___2404251100/URN:NASA:UNITY:UDS_LOCAL_TEST:DEV:UDS_COLLECTION___2404251100:abcd.1234.efgh.test_file05/abcd.1234.efgh.test_file05.data.stac.json",
                        "checksumType": "md5",
                        "checksum": "<Currently checksum and size of some files are unknown and -1. It needs to be fixed or omitted>",
                        "size": -1
                    },
                    {
                        "name": "abcd.1234.efgh.test_file05.nc.cas",
                        "type": "metadata",
                        "uri": "s3://unity-dev-cumulus-unity-william-test-1/URN:NASA:UNITY:UDS_LOCAL_TEST:DEV:UDS_COLLECTION___2404251100/URN:NASA:UNITY:UDS_LOCAL_TEST:DEV:UDS_COLLECTION___2404251100:abcd.1234.efgh.test_file05/abcd.1234.efgh.test_file05.nc.cas",
                        "checksumType": "md5",
                        "checksum": "unknown",
                        "size": -1
                    },
                    {
                        "name": "abcd.1234.efgh.test_file05.nc.stac.json",
                        "type": "metadata",
                        "uri": "s3://unity-dev-cumulus-unity-william-test-1/URN:NASA:UNITY:UDS_LOCAL_TEST:DEV:UDS_COLLECTION___2404251100/URN:NASA:UNITY:UDS_LOCAL_TEST:DEV:UDS_COLLECTION___2404251100:abcd.1234.efgh.test_file05/abcd.1234.efgh.test_file05.nc.stac.json",
                        "checksumType": "md5",
                        "checksum": "unknown",
                        "size": -1
                    }
                ]
            }
        }
        self.__response_message = {
            'submissionTime': TimeUtils.get_current_time(),
            'receivedTime': validated_input_event['submissionTime'],
            'processCompleteTime': TimeUtils.get_current_time(),
            'collection': validated_input_event['collection'],
            'identifier': validated_input_event['identifier'],
        }
        self.__send_random_result()
        return
