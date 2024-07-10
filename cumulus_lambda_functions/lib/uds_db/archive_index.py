from cumulus_lambda_functions.lib.uds_db.db_constants import DBConstants

from cumulus_lambda_functions.lib.aws.es_abstract import ESAbstract

from cumulus_lambda_functions.lib.aws.es_factory import ESFactory


class UdsArchiveConfigIndex:
    def __init__(self, es_url, es_port=443):
        self.__es: ESAbstract = ESFactory().get_instance('AWS',
                                                         index=None,
                                                         base_url=es_url,
                                                         port=es_port)
        self.__tenant, self.__venue = '', ''

    def set_tenant_venue(self, tenant, venue):
        self.__tenant, self.__venue = tenant, venue
        return self

    def get_config(self, collection_id):
        read_alias_name = f'{DBConstants.granules_read_alias_prefix}_{self.__tenant}_{self.__venue}_perc'.lower().strip()
        result = self.__es.query({
            'size': 9999,
            'query': {
                'bool': {
                    'must': [{
                        "term": {"collection": {"value": collection_id}}
                    }]
                }
            }
        }, read_alias_name)
        return result

    def add_new_config(self, collection_id, daac_collection_id, daac_sns_topic_arn, username):
        write_alias_name = f'{DBConstants.granules_write_alias_prefix}_{self.__tenant}_{self.__venue}_perc'.lower().strip()
        result = self.__es.index_one({
            "collection": collection_id,
            "daac_collection_name": daac_collection_id,
            "daac_sns_topic_arn": daac_sns_topic_arn,
            "ss_query": {
                "bool": {
                    "must": [{
                        "term": {"collection": {"value": collection_id} }
                    }]
                }
            },
            "ss_username": username,
        }, f'{daac_collection_id}__{collection_id}', index=write_alias_name)
        return

    def delete_config(self, collection_id, daac_collection_id):
        write_alias_name = f'{DBConstants.granules_write_alias_prefix}_{self.__tenant}_{self.__venue}_perc'.lower().strip()
        self.__es.delete_by_query({
            'size': 9999,
            'query': {
                'bool': {
                    'must': [
                        { "term": {"collection": {"value": collection_id}} },
                        {"term": {"daac_collection_name": {"value": daac_collection_id}}},
                    ]
                }
            }
        }, write_alias_name)
        return

    def update_config(self, collection_id, daac_collection_id, daac_sns_topic_arn, username):
        write_alias_name = f'{DBConstants.granules_write_alias_prefix}_{self.__tenant}_{self.__venue}_perc'.lower().strip()
        result = self.__es.update_one({
            "collection": collection_id,
            "daac_collection_name": daac_collection_id,
            "daac_sns_topic_arn": daac_sns_topic_arn,
            "ss_query": {
                "bool": {
                    "must": [{
                        "term": {"collection": {"value": collection_id}}
                    }]
                }
            },
            "ss_username": username,
        }, f'{daac_collection_id}__{collection_id}', index=write_alias_name)
        return
