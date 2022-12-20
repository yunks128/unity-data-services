import json
import logging
import os
from glob import glob

from cumulus_lambda_functions.lib.lambda_logger_generator import LambdaLoggerGenerator

from cumulus_lambda_functions.lib.aws.es_abstract import ESAbstract

from cumulus_lambda_functions.lib.aws.es_factory import ESFactory

from cumulus_lambda_functions.lib.uds_db.db_constants import DBConstants

LOGGER = LambdaLoggerGenerator.get_logger(__name__, LambdaLoggerGenerator.get_level_from_env())


class SetupESIndexAlias:
    def __init__(self):
        required_env = ['ES_URL']
        if not all([k in os.environ for k in required_env]):
            raise EnvironmentError(f'one or more missing env: {required_env}')
        self.__es: ESAbstract = ESFactory().get_instance('AWS',
                                                         index=DBConstants.collections_index,
                                                         base_url=os.getenv('ES_URL'),
                                                         port=int(os.getenv('ES_PORT', '443'))
                                                         )

    def get_index_mapping(self, index_name: str, index_files: dict):
        if index_name not in index_files:
            raise ValueError(f'missing index_name: {index_name} in {index_files}')
        with open(index_files[index_name], 'r') as ff:
            index_json = json.loads(ff.read())
        return index_json

    def start(self):
        current_dir = os.path.dirname(__file__)
        index_files = glob(os.path.join(current_dir, 'elasticsearch_index', '*.json'))
        index_files = {os.path.basename(k): k for k in index_files}
        if 'alias_pointer.json' not in index_files:
            raise ValueError(f'missing alias_pointer.json in {index_files}')
        with open(index_files['alias_pointer.json'], 'r') as ff:
            alias_json = json.loads(ff.read())
        alias_json = [k['add'] for k in alias_json['actions']]
        for each_action in alias_json:
            current_index = each_action['index']
            current_alias = each_action['alias']
            LOGGER.debug(f'working on {current_index}')
            index_json = self.get_index_mapping(current_index, index_files)
            self.__es.create_index(current_index, index_json)
            self.__es.create_alias(current_index, current_alias)
        return self
