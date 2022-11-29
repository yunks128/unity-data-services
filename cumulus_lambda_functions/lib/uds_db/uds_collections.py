import logging
from collections import namedtuple
from datetime import datetime

from cumulus_lambda_functions.lib.aws.es_abstract import ESAbstract
from cumulus_lambda_functions.lib.aws.es_factory import ESFactory
from cumulus_lambda_functions.lib.uds_db.db_constants import DBConstants
LOGGER = logging.getLogger(__name__)


CollectionIdentifier = namedtuple('CollectionIdentifier', ['urn', 'nasa', 'project', 'tenant', 'venue', 'id'])


class UdsCollections:
    collection_id = 'collection_id'
    bbox = 'bbox'
    granule_count = 'granule_count'
    start_time = 'start_time'
    end_time = 'end_time'

    def __init__(self, es_url, es_port=443):
        self.__es: ESAbstract = ESFactory().get_instance('AWS',
                                                         index=DBConstants.collections_index,
                                                         base_url=es_url,
                                                         port=es_port)

    @staticmethod
    def decode_identifier(incoming_identifier: str) -> CollectionIdentifier:
        return CollectionIdentifier._make(incoming_identifier.split(':'))

    def __bbox_to_polygon(self, bbox: list):
        if len(bbox) != 4:
            raise ValueError(f'not bounding box: {bbox}')
        # ['min lon', 'min lat', 'max lon', 'max lat']
        polygon = [
            [bbox[1], bbox[0]],  # min lat, min lon
            [bbox[1], bbox[2]],  # min lat, max lon
            [bbox[3], bbox[2]],  # max lat, max lon
            [bbox[3], bbox[0]],  # max lat, min lon
            [bbox[1], bbox[0]],  # min lat, min lon
        ]
        return polygon
    def add_collection(self, collection_id: str, start_time: int, end_time: int, bbox: list, granules_count: int=0):
        self.__es.index_one({
            self.collection_id: collection_id,
            self.granule_count: granules_count,
            self.start_time: start_time,
            self.end_time: end_time,
            self.bbox: {
                'type': 'polygon',
                'coordinates': [self.__bbox_to_polygon(bbox)]
            },
        }, collection_id, DBConstants.collections_index)
        return self

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
