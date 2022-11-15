import logging
from collections import namedtuple

from cumulus_lambda_functions.lib.aws.es_abstract import ESAbstract
from cumulus_lambda_functions.lib.aws.es_factory import ESFactory
from cumulus_lambda_functions.lib.uds_db.db_constants import DBConstants
LOGGER = logging.getLogger(__name__)


CollectionIdentifier = namedtuple('CollectionIdentifier', ['urn', 'nasa', 'project', 'tenant', 'venue', 'id'])


class UdsCollections:
    collection_id = 'collection_id'
    def __init__(self, es_url, es_port=443):
        self.__es: ESAbstract = ESFactory().get_instance('AWS',
                                                         index=DBConstants.collections_index,
                                                         base_url=es_url,
                                                         port=es_port)

    @staticmethod
    def decode_identifier(incoming_identifier: str) -> CollectionIdentifier:
        return CollectionIdentifier._make(incoming_identifier.split(':'))

    def get_collections(self, collection_regex: list):
        authorized_collection_ids_dsl = {
            'query': {
                'bool': {
                    'should': [
                        {'regexp': {DBConstants.collection_id: k}} for k in collection_regex
                    ]
                }
            },
            'sort': [
                {DBConstants.collection_id: {'order': 'asc'}}
            ]
        }
        authorized_collection_ids = self.__es.query_pages(authorized_collection_ids_dsl, DBConstants.collections_index)
        authorized_collection_ids = [k['_source'] for k in authorized_collection_ids['hits']['hits']]
        return authorized_collection_ids
