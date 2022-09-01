from datetime import datetime

import pystac
from pystac import Link, Collection, Extent, SpatialExtent, TemporalExtent, Summaries

from cumulus_lambda_functions.cumulus_stac.collection_transformer import CollectionTransformer
from cumulus_lambda_functions.lib.lambda_logger_generator import LambdaLoggerGenerator

LOGGER = LambdaLoggerGenerator.get_logger(__name__, LambdaLoggerGenerator.get_level_from_env())


class CumulusCollectionDapaCreation:
    def __init__(self):
        self.__id = ''
        self.__granule_id_extraction_regex = ''
        self.__process = ''
        self.__collection_title = ''
        self.__granule_id_regex = ''
        self.__sample_filename = ''
        self.__files = []
        self.__collection_transformer = CollectionTransformer()

    def with_title(self, title: str):
        self.__collection_title = title
        return self

    def with_process(self, process: str):
        self.__process = process
        return self

    def with_id(self, collection_id: str):
        self.__id = collection_id
        if '___' not in collection_id:
            LOGGER.warning(f'no ID in {collection_id}. using 001')
            self.__id = f'{self.__id}___001'
        return self

    def with_graule_id_regex(self, granule_id_regex):
        self.__granule_id_regex = granule_id_regex
        return self

    def with_granule_id_extraction_regex(self, granule_id_extraction_regex):
        self.__granule_id_extraction_regex = granule_id_extraction_regex
        return self

    def add_file_type(self, title: str, regex: str, bucket: str, media_type: str, rel: str = 'item'):
        if rel == 'root':
            LOGGER.debug('updating media_type for rel = root')
            media_type = 'application/json'
        self.__files.append(Link(rel=rel, target=self.__collection_transformer.generate_target_link_url(regex, bucket), media_type=media_type, title=title))
        return self

    def start(self):
        # TODO validate
        stac_collection = Collection(id=self.__id,
                                     description='TODO',
                                     extent=Extent(SpatialExtent([0, 0, 0, 0]),
                                                   TemporalExtent([[datetime.utcnow(), datetime.utcnow()]])),
                                     title=self.__collection_title,
                                     summaries=Summaries({
                                         'granuleId': [self.__granule_id_regex],
                                         'granuleIdExtraction': [self.__granule_id_extraction_regex],
                                         'process': [self.__process]
                                     }),
                                     )
        stac_collection.add_links(self.__files)
        return stac_collection.to_dict(include_self_link=False)
