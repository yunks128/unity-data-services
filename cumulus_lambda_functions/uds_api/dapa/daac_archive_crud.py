import os
from cumulus_lambda_functions.lib.uds_db.archive_index import UdsArchiveConfigIndex


class DaacArchiveCrud:
    def __init__(self, authorization_info, request_body):
        self.__request_body = request_body
        self.__authorization_info = authorization_info
        required_env = ['ES_URL', 'ADMIN_COMMA_SEP_GROUPS']
        if not all([k in os.environ for k in required_env]):
            raise EnvironmentError(f'one or more missing env: {required_env}')
        self.__es_url = os.getenv('ES_URL')
        self.__es_port = int(os.getenv('ES_PORT', '443'))
        self.__daac_config = UdsArchiveConfigIndex(self.__es_url, self.__es_port)

    def add_new_config(self):
        result = self.__daac_config.add_new_config()
        return {
            'statusCode': 200,
            'body': {'message': 'inserted'}
        }
