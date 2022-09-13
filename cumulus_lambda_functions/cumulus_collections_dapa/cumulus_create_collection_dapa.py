import json
import os
from threading import Thread

import pystac

from cumulus_lambda_functions.cumulus_stac.collection_transformer import CollectionTransformer
from cumulus_lambda_functions.cumulus_wrapper.query_collections import CollectionsQuery
from cumulus_lambda_functions.lib.lambda_logger_generator import LambdaLoggerGenerator

LOGGER = LambdaLoggerGenerator.get_logger(__name__, LambdaLoggerGenerator.get_level_from_env())


class CollectionCreationThread(Thread):
    def __init__(self, request_body):
        super().__init__()
        # self.thread_name = thread_name
        # self.thread_ID = thread_ID
        self.__request_body = request_body
        self.__cumulus_collection_query = CollectionsQuery('', '')
        self.__cumulus_lambda_prefix = os.getenv('CUMULUS_LAMBDA_PREFIX')
        self.__ingest_sqs_url = os.getenv('CUMULUS_WORKFLOW_SQS_URL')
        self.__workflow_name = os.getenv('CUMULUS_WORKFLOW_NAME', 'CatalogGranule')
        self.__provider_id = ''  # TODO. need this?
        # helper function to execute the threads

    def run(self):
        try:
            cumulus_collection_doc = CollectionTransformer().from_stac(self.__request_body)
            creation_result = self.__cumulus_collection_query.create_collection(cumulus_collection_doc, self.__cumulus_lambda_prefix)
            if 'status' not in creation_result:
                LOGGER.error(f'status not in creation_result: {creation_result}')
                return

            rule_creation_result = self.__cumulus_collection_query.create_sqs_rules(
                cumulus_collection_doc,
                self.__cumulus_lambda_prefix,
                self.__ingest_sqs_url,
                self.__provider_id,
                self.__workflow_name,
            )
            if 'status' not in rule_creation_result:
                # 'TODO' delete collection
                LOGGER.error(f'status not in rule_creation_result: {rule_creation_result}')
                return
        except Exception as e:
            LOGGER.exception('error while creating new collection in Cumulus')
            return
        LOGGER.info(f'collection and rule created for: {self.__request_body}')
        return


class CumulusCreateCollectionDapa:
    def __init__(self, event):
        required_env = ['CUMULUS_LAMBDA_PREFIX', 'CUMULUS_WORKFLOW_SQS_URL']
        if not all([k in os.environ for k in required_env]):
            raise EnvironmentError(f'one or more missing env: {required_env}')
        self.__event = event
        self.__request_body = None
        self.__cumulus_collection_query = CollectionsQuery('', '')
        self.__cumulus_lambda_prefix = os.getenv('CUMULUS_LAMBDA_PREFIX')
        self.__ingest_sqs_url = os.getenv('CUMULUS_WORKFLOW_SQS_URL')
        self.__workflow_name = os.getenv('CUMULUS_WORKFLOW_NAME', 'CatalogGranule')
        self.__provider_id = ''  # TODO. need this?

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
        # thread1 = CollectionCreationThread(self.__request_body)
        # LOGGER.info(f'starting background thread')
        # thread1.start()
        # LOGGER.info(f'not waiting for background thread')
        # return {
        #     'statusCode': 202,
        #     'body': {'message': 'started in backgorund'}
        # }
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
            rule_creation_result = self.__cumulus_collection_query.create_sqs_rules(
                cumulus_collection_doc,
                self.__cumulus_lambda_prefix,
                self.__ingest_sqs_url,
                self.__provider_id,
                self.__workflow_name,
            )
            if 'status' not in rule_creation_result:
                # 'TODO' delete collection
                return {
                    'statusCode': 500,
                    'body': {
                        'message': {rule_creation_result},
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
