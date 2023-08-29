import os

from cumulus_lambda_functions.lib.lambda_logger_generator import LambdaLoggerGenerator

from cumulus_lambda_functions.lib.aws.es_abstract import ESAbstract

from cumulus_lambda_functions.lib.aws.es_factory import ESFactory

from cumulus_lambda_functions.lib.uds_db.db_constants import DBConstants
LOGGER = LambdaLoggerGenerator.get_logger(__name__, LambdaLoggerGenerator.get_level_from_env())


class GranulesDbIndex:
    def __init__(self):
        required_env = ['ES_URL']
        if not all([k in os.environ for k in required_env]):
            raise EnvironmentError(f'one or more missing env: {required_env}')
        self.__es: ESAbstract = ESFactory().get_instance('AWS',
                                                         index=DBConstants.collections_index,
                                                         base_url=os.getenv('ES_URL'),
                                                         port=int(os.getenv('ES_PORT', '443'))
                                                         )

    def create_new_index(self, tenant, tenant_venue, es_mapping: dict):
        # TODO validate es_mapping
        # get current version from alias
        # if not found, create a new alias
        # get base definition.
        # add custom definition
        # create new version
        # throw error and return if fails
        # add new index to read alias
        # add new index to write alias
        # add delete current index from write alias
        tenant = tenant.replace(':', '--')
        write_alias_name = f'{DBConstants.granules_write_alias_prefix}_{tenant}_{tenant_venue}'.lower().strip()
        read_alias_name = f'{DBConstants.granules_read_alias_prefix}_{tenant}_{tenant_venue}'.lower().strip()
        current_alias = self.__es.get_alias(write_alias_name)
        # {'meta_labels_v2': {'aliases': {'metadata_labels': {}}}}
        current_index_name = f'{write_alias_name}__v0' if current_alias == {} else [k for k in current_alias.keys()][0]
        new_version = int(current_index_name.split('__')[-1][1:]) + 1
        new_index_name = f'{DBConstants.granules_index_prefix}_{tenant}_{tenant_venue}__v{new_version:02d}'.lower().strip()
        LOGGER.debug(f'new_index_name: {new_index_name}')
        index_mapping = {
            "settings": {
                "number_of_shards": 3,
                "number_of_replicas": 2
            },
            "mappings": {
                "dynamic": "strict",
                "properties": {
                    **es_mapping,
                    "granule_id": {"type": "keyword"},
                    "collection_id": {"type": "keyword"}
                }
            }
        }
        self.__es.create_index(new_index_name, index_mapping)
        self.__es.create_alias(new_index_name, read_alias_name)
        self.__es.swap_index_for_alias(write_alias_name, current_index_name, new_index_name)
        return

    def destroy_indices(self, tenant, tenant_venue):
        # TODO assuming that both read and write aliases are destroyed once indices are destroyed.
        tenant = tenant.replace(':', '--')
        write_alias_name = f'{DBConstants.granules_write_alias_prefix}_{tenant}_{tenant_venue}'.lower().strip()
        actual_write_alias = self.__es.get_alias(write_alias_name)
        for each_index in actual_write_alias.keys():
            LOGGER.debug(f'deleting index: {each_index}')
            self.__es.delete_index(each_index)
        return
