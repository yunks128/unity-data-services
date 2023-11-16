import json
import os

from cumulus_lambda_functions.lib.aws.es_middleware import ESMiddleware

from cumulus_lambda_functions.cumulus_stac.item_transformer import ItemTransformer
from cumulus_lambda_functions.lib.cql_parser import CqlParser

from cumulus_lambda_functions.lib.uds_db.uds_collections import UdsCollections

from cumulus_lambda_functions.lib.json_validator import JsonValidator

from cumulus_lambda_functions.lib.lambda_logger_generator import LambdaLoggerGenerator

from cumulus_lambda_functions.cumulus_wrapper.query_granules import GranulesQuery
from cumulus_lambda_functions.uds_api.dapa.granules_db_index import GranulesDbIndex

LOGGER = LambdaLoggerGenerator.get_logger(__name__, LambdaLoggerGenerator.get_level_from_env())


class GranulesDapaQuery:
    def __init__(self, collection_id, limit, offset, input_datetime, filter_input, pagination_links):
        self.__input_datetime = input_datetime
        self.__pagination_links = pagination_links
        self.__collection_id = collection_id
        self.__limit = limit
        self.__offset = offset
        self.__filter_input = filter_input
        self.__granules_index = GranulesDbIndex()

    def __generate_es_dsl(self):
        query_terms = [
            {'term': {'collection': {'value': self.__collection_id}}}
        ]
        query_terms.extend(self.__get_time_range_terms())
        if self.__filter_input is not None:
            query_terms.append(CqlParser('properties').transform(self.__filter_input))
        query_dsl = {
            'track_total_hits': True,
            'size': self.__limit,
            'sort': [{'id': {'order': 'asc'}}],
            'query': {
                'bool': {
                    'must': query_terms
                }
            }
        }
        if self.__offset is not None:
            query_dsl['search_after'] = [k.strip() for k in self.__offset.split(',')]
        LOGGER.debug(f'query_dsl: {query_dsl}')
        return query_dsl

    def __get_time_range_terms(self):
        if self.__input_datetime is None:
            return []
        if '/' not in self.__input_datetime:
            return [
                {'range': {'properties.start_datetime': {'lte': self.__input_datetime}}},
                {'range': {'properties.end_datetime': {'gte': self.__input_datetime}}},
            ]
        split_time_range = [k.strip() for k in self.__input_datetime.split('/')]
        if split_time_range[0] == '..':
            return [
                {'range': {'properties.end_datetime': {'gte': split_time_range[1]}}},
            ]
        if split_time_range[1] == '..':
            return [
                {'range': {'properties.start_datetime': {'lte': split_time_range[0]}}},
            ]
        return [
                {'range': {'properties.start_datetime': {'lte': split_time_range[1]}}},
                {'range': {'properties.end_datetime': {'gte': split_time_range[0]}}},
            ]

    def __get_size(self):
        try:
            cumulus_size = self.__cumulus.get_size(self.__cumulus_lambda_prefix)
        except:
            LOGGER.exception(f'cannot get cumulus_size')
            cumulus_size = {'total_size': -1}
        return cumulus_size

    def start(self):
        try:
            granules_query_dsl = self.__generate_es_dsl()
            LOGGER.debug(f'granules_query_dsl: {granules_query_dsl}')
            collection_identifier = UdsCollections.decode_identifier(self.__collection_id)
            granules_query_result = GranulesDbIndex().dsl_search(collection_identifier.tenant,
                                                                  collection_identifier.venue,
                                                                  granules_query_dsl)
            LOGGER.debug(f'granules_query_result: {granules_query_result}')
            result_size = ESMiddleware.get_result_size(granules_query_result)
            granules_query_result = [k['_source'] for k in granules_query_result['hits']['hits']]

            return {
                'statusCode': 200,
                'body': {
                    'numberMatched': {'total_size': result_size},
                    'numberReturned': len(granules_query_result),
                    'stac_version': '1.0.0',
                    'type': 'FeatureCollection',  # TODO correct name?
                    'links': self.__pagination_links,
                    'features': granules_query_result
                }
            }
        except Exception as e:
            LOGGER.exception(f'unexpected error')
            return {
                'statusCode': 500,
                'body': {'message': f'unpredicted error: {str(e)}'}
            }
