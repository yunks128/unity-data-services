import json
import os

import pystac
from cumulus_lambda_functions.cumulus_wrapper.query_collections import CollectionsQuery

from cumulus_lambda_functions.cumulus_stac.collection_transformer import CollectionTransformer

from cumulus_lambda_functions.lib.aws.aws_lambda import AwsLambda

from cumulus_lambda_functions.lib.lambda_logger_generator import LambdaLoggerGenerator
from pydantic import BaseModel
LOGGER = LambdaLoggerGenerator.get_logger(__name__, LambdaLoggerGenerator.get_level_from_env())


class CollectionDapaCreation:
    def __init__(self, request_body):
        required_env = ['CUMULUS_LAMBDA_PREFIX', 'CUMULUS_WORKFLOW_SQS_URL']
        if not all([k in os.environ for k in required_env]):
            raise EnvironmentError(f'one or more missing env: {required_env}')

        self.__request_body = request_body
        self.__cumulus_lambda_prefix = os.getenv('CUMULUS_LAMBDA_PREFIX')
        self.__ingest_sqs_url = os.getenv('CUMULUS_WORKFLOW_SQS_URL')
        self.__workflow_name = os.getenv('CUMULUS_WORKFLOW_NAME', 'CatalogGranule')
        self.__provider_id = os.getenv('UNITY_DEFAULT_PROVIDER', '')

    def create(self):
        try:
            cumulus_collection_query = CollectionsQuery('', '')

            collection_transformer = CollectionTransformer()
            cumulus_collection_doc = collection_transformer.from_stac(self.__request_body)
            self.__provider_id = self.__provider_id if collection_transformer.output_provider is None else collection_transformer.output_provider
            creation_result = cumulus_collection_query.create_collection(cumulus_collection_doc, self.__cumulus_lambda_prefix)
            if 'status' not in creation_result:
                LOGGER.error(f'status not in creation_result: {creation_result}')
                return {
                    'statusCode': 500,
                    'body': json.dumps({
                        'message': creation_result
                    })
                }
            LOGGER.debug(f'__provider_id: {self.__provider_id}')
            rule_creation_result = cumulus_collection_query.create_sqs_rules(
                cumulus_collection_doc,
                self.__cumulus_lambda_prefix,
                self.__ingest_sqs_url,
                self.__provider_id,
                self.__workflow_name,
            )
            if 'status' not in rule_creation_result:
                LOGGER.error(f'status not in rule_creation_result. deleting collection: {rule_creation_result}')
                delete_collection_result = cumulus_collection_query.delete_collection(self.__cumulus_lambda_prefix, cumulus_collection_doc['name'], cumulus_collection_doc['version'])
                return {
                    'statusCode': 500,
                    'body': json.dumps({
                        'message': {rule_creation_result},
                        'details': f'collection deletion result: {delete_collection_result}'
                    })
                }
        except Exception as e:
            LOGGER.exception('error while creating new collection in Cumulus')
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'message': f'error while creating new collection in Cumulus. check details',
                    'details': str(e)
                })
            }
        LOGGER.info(f'creation_result: {creation_result}')
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': creation_result
            })
        }

    def start(self, current_url):
        LOGGER.debug(f'request body: {self.__request_body}')
        validation_result = pystac.Collection.from_dict(self.__request_body).validate()
        if not isinstance(validation_result, list):
            LOGGER.error(f'request body is not valid STAC collection: {validation_result}')
            return {
                'statusCode': 500,
                'body': json.dumps({'message': f'request body is not valid STAC Collection schema. check details',
                         'details': validation_result})
            }
        # # TODO really trigger this
        # event = {
        #
        # }
        # response = AwsLambda().invoke_function(
        #     function_name=self.__collection_creation_lambda_name,
        #     payload=self.__event,
        # )
        # LOGGER.debug(f'async function started: {response}')
        return {
            'statusCode': 202,
            'body': json.dumps({
                'message': 'processing'
            })
        }
