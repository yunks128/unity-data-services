import json
import os

from cumulus_lambda_functions.lib.authorization.uds_authorizer_abstract import UDSAuthorizorAbstract
from cumulus_lambda_functions.lib.authorization.uds_authorizer_factory import UDSAuthorizerFactory
from cumulus_lambda_functions.lib.json_validator import JsonValidator

from cumulus_lambda_functions.lib.lambda_logger_generator import LambdaLoggerGenerator
from cumulus_lambda_functions.lib.uds_db.db_constants import DBConstants

LOGGER = LambdaLoggerGenerator.get_logger(__name__, LambdaLoggerGenerator.get_level_from_env())


delete_schema = {
    'type': 'object',
    'required': ['tenant', 'venue', 'group_name'],
    'properties': {
        'tenant': {'type': 'string'},
        'venue': {'type': 'string'},
        'group_name': {'type': 'string'},
    }
}

add_schema = {
    'type': 'object',
    'required': ['tenant', 'venue', 'group_name', 'actions', 'resources'],
    'properties': {
        'tenant': {'type': 'string'},
        'venue': {'type': 'string'},
        'group_name': {'type': 'string'},
        'actions': {
            'type': 'array',
            'items': {
                'type': 'string',
                'enum': [DBConstants.create, DBConstants.delete, DBConstants.update, DBConstants.read]
            },
            'minItems': 1,
        },
        'resources': {
            'type': 'array',
            'items': {'type': 'string'},
            'minItems': 1,
        },
    }

}


class AuthCrud:
    def __init__(self, event):
        required_env = ['ES_URL']
        if not all([k in os.environ for k in required_env]):
            raise EnvironmentError(f'one or more missing env: {required_env}')
        self.__event = event
        self.__request_body = {}
        self.__es_url = os.getenv('ES_URL')
        self.__es_port = int(os.getenv('ES_PORT', '443'))
        self.__authorizer: UDSAuthorizorAbstract = UDSAuthorizerFactory() \
            .get_instance(UDSAuthorizerFactory.cognito,
                          user_pool_id='NA',
                          es_url=self.__es_url,
                          es_port=self.__es_port
                          )

    def __load_request_body(self):
        if 'body' not in self.__event:
            raise ValueError(f'missing body in {self.__event}')
        self.__request_body = json.loads(self.__event['body'])
        return

    def list_all_record(self):
        self.__load_request_body()
        return

    def add_new_record(self):
        self.__load_request_body()
        body_validator_result = JsonValidator(add_schema).validate(self.__request_body)
        if body_validator_result is not None:
            LOGGER.error(f'invalid add body: {body_validator_result}. request_body: {self.__request_body}')
            return {
                'statusCode': 500,
                'body': f'invalid add body: {body_validator_result}. request_body: {self.__request_body}'
            }
        self.__authorizer.add_authorized_group(
            action=self.__request_body['actions'],
            resource=self.__request_body['resources'],
            tenant=self.__request_body['tenant'],
            venue=self.__request_body['venue'],
            ldap_group_name=self.__request_body['group_name'],
        )
        return {
            'statusCode': 200,
            'body': 'deleted'
        }

    def update_record(self):
        self.__load_request_body()
        body_validator_result = JsonValidator(add_schema).validate(self.__request_body)
        if body_validator_result is not None:
            LOGGER.error(f'invalid update body: {body_validator_result}. request_body: {self.__request_body}')
            return {
                'statusCode': 500,
                'body': f'invalid update body: {body_validator_result}. request_body: {self.__request_body}'
            }
        self.__authorizer.update_authorized_group(
            action=self.__request_body['actions'],
            resource=self.__request_body['resources'],
            tenant=self.__request_body['tenant'],
            venue=self.__request_body['venue'],
            ldap_group_name=self.__request_body['group_name'],
        )
        return {
            'statusCode': 200,
            'body': 'deleted'
        }

    def delete_record(self):
        self.__load_request_body()
        body_validator_result = JsonValidator(delete_schema).validate(self.__request_body)
        if body_validator_result is not None:
            LOGGER.error(f'invalid delete body: {body_validator_result}. request_body: {self.__request_body}')
            return {
                'statusCode': 500,
                'body': f'invalid delete body: {body_validator_result}. request_body: {self.__request_body}'
            }
        self.__authorizer.delete_authorized_group(
            tenant=self.__request_body.get('tenant'),
            venue=self.__request_body.get('venue'),
            ldap_group_name=self.__request_body.get('group_name'),
        )
        return {
            'statusCode': 200,
            'body': 'deleted'
        }
