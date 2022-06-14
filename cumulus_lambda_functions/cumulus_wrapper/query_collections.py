import json

import requests
from cumulus_lambda_functions.cumulus_stac.collection_transformer import CollectionTransformer

from cumulus_lambda_functions.cumulus_wrapper.cumulus_base import CumulusBase
from cumulus_lambda_functions.lib.lambda_logger_generator import LambdaLoggerGenerator

LOGGER = LambdaLoggerGenerator.get_logger(__name__, LambdaLoggerGenerator.get_level_from_env())


class CollectionsQuery(CumulusBase):
    __collections_key = 'collections'

    def __init__(self, cumulus_base: str, cumulus_token: str):
        super().__init__(cumulus_base, cumulus_token)

    def query_direct_to_private_api(self, private_api_prefix: str):
        payload = {
            'httpMethod': 'GET',
            'resource': '/{proxy+}',
            'path': f'/{self.__collections_key}',
            'queryStringParameters': {k[0]: k[1] for k in [k1.split('=') for k1 in self._conditions]},
        }
        LOGGER.debug(f'payload: {payload}')
        try:
            query_result = self._invoke_api(payload, private_api_prefix)
            """
        {'statusCode': 200, 'body': '{"meta":{"name":"cumulus-api","stack":"am-uds-dev-cumulus","table":"granule","limit":3,"page":1,"count":0},"results":[]}', 'headers': {'x-powered-by': 'Express', 'access-control-allow-origin': '*', 'strict-transport-security': 'max-age=31536000; includeSubDomains', 'content-type': 'application/json; charset=utf-8', 'content-length': '120', 'etag': 'W/"78-YdHqDNIH4LuOJMR39jGNA/23yOQ"', 'date': 'Tue, 07 Jun 2022 22:30:44 GMT', 'connection': 'close'}, 'isBase64Encoded': False}
            """
            if query_result['statusCode'] >= 500:
                LOGGER.error(f'server error status code: {query_result.statusCode}. details: {query_result}')
                return {'server_error': query_result}
            if query_result['statusCode'] >= 400:
                LOGGER.error(f'client error status code: {query_result.statusCode}. details: {query_result}')
                return {'client_error': query_result}
            query_result = json.loads(query_result['body'])
            LOGGER.debug(f'json query_result: {query_result}')
            if 'results' not in query_result:
                LOGGER.error(f'missing key: results. invalid response json: {query_result}')
                return {'server_error': f'missing key: results. invalid response json: {query_result}'}
            query_result = query_result['results']
            stac_list = [CollectionTransformer().to_stac(k) for k in query_result]
            LOGGER.debug(f'stac_list: {stac_list}')
        except Exception as e:
            LOGGER.exception('error while invoking')
            return {'server_error': f'error while invoking:{str(e)}'}
        return {'results': stac_list}

    def query(self):
        conditions_str = '&'.join(self._conditions)
        LOGGER.info(f'cumulus_base: {self.cumulus_base}')
        LOGGER.info(f'get_base_headers: {self.get_base_headers()}')
        try:
            query_result = requests.get(url=f'{self.cumulus_base}/{self.__collections_key}?{conditions_str}', headers=self.get_base_headers())
            LOGGER.info(f'query_result: {query_result}')
            if query_result.status_code >= 500:
                return {'server_error': query_result.text}
            if query_result.status_code >= 400:
                return {'client_error': query_result.text}
            query_result = json.loads(query_result.content.decode())
            LOGGER.info(f'query_result: {query_result}')
            if 'results' not in query_result:
                return {'server_error': f'missing key: results. invalid response json: {query_result}'}
            query_result = query_result['results']
        except Exception as e:
            LOGGER.exception('error during cumulus query')
            return {'server_error': str(e)}
        return {'results': [CollectionTransformer().to_stac(k) for k in query_result]}
