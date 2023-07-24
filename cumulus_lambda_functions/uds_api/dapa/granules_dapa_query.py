import json
import os

from cumulus_lambda_functions.lib.utils.lambda_api_gateway_utils import LambdaApiGatewayUtils

from cumulus_lambda_functions.lib.json_validator import JsonValidator

from cumulus_lambda_functions.lib.lambda_logger_generator import LambdaLoggerGenerator

from cumulus_lambda_functions.cumulus_wrapper.query_granules import GranulesQuery
LOGGER = LambdaLoggerGenerator.get_logger(__name__, LambdaLoggerGenerator.get_level_from_env())


class GranulesDapaQuery:
    def __init__(self, collection_id, limit, offset, datetime, filter_input, pagination_links):
        self.__pagination_links = pagination_links
        page_number = (offset // limit) + 1
        if 'CUMULUS_LAMBDA_PREFIX' not in os.environ:
            raise EnvironmentError('missing key: CUMULUS_LAMBDA_PREFIX')

        self.__cumulus_lambda_prefix = os.getenv('CUMULUS_LAMBDA_PREFIX')
        self.__cumulus = GranulesQuery('https://na/dev', 'NA')
        self.__cumulus.with_limit(limit)
        self.__cumulus.with_page_number(page_number)
        self.__cumulus.with_collection_id(collection_id)
        self.__get_time_range(datetime)
        self.__get_filter(filter_input)

    def __get_time_range(self, datetime: str):
        if datetime is None:
            return self
        if '/' not in datetime:
            self.__cumulus.with_time(datetime)
            return self
        split_time_range = [k.strip() for k in datetime.split('/')]
        if split_time_range[0] == '..':
            self.__cumulus.with_time_to(split_time_range[1])
            return
        if split_time_range[1] == '..':
            self.__cumulus.with_time_from(split_time_range[0])
            return
        self.__cumulus.with_time_range(split_time_range[0], split_time_range[1])
        return self

    def __get_filter(self, filter_input: str):
        """
        https://portal.ogc.org/files/96288#rc_filter

        { "eq": [ { "property": "city" }, "Toronto" ] }

        {
          "like": [
            { "property": "name" },
            "Smith."
          ],
          "singleChar": ".",
          "nocase": true
        }

        {
  "in": {
     "value": { "property": "cityName" },
     "list": [ "Toronto", "Franfurt", "Tokyo", "New York" ],
     "nocase": false
  }
}
        :return:
        """
        if filter_input is None:
            return self
        filter_event = json.loads(filter_input)
        if 'in' not in filter_event:
            return self
        schema = {
            "type": {
                "required": ["in"],
                "properties": {
                    "in": {
                        "type": "object",
                        "required": ["value", "list"],
                        "properties": {
                            "value": {
                                "type": "object",
                                "required": ["property"],
                                "properties": {
                                    "property": {"type": "string"}
                                }
                            },
                            "list": {
                                "type": "array",
                                "minItems": 1,
                                "items": {
                                    "type": "string"
                                }
                            }
                        }
                    }
                }
            }
        }
        filter_event_validator_result = JsonValidator(schema).validate(filter_event)
        if filter_event_validator_result is not None:
            LOGGER.error(f'invalid event: {filter_event_validator_result}. event: {filter_event}')
            return self
        search_key = filter_event['in']['value']['property']
        search_values = filter_event['in']['value']['list']
        self.__cumulus.with_filter(search_key, search_values)
        return self

    def __get_size(self):
        try:
            cumulus_size = self.__cumulus.get_size(self.__cumulus_lambda_prefix)
        except:
            LOGGER.exception(f'cannot get cumulus_size')
            cumulus_size = {'total_size': -1}
        return cumulus_size

    def start(self):
        try:
            cumulus_result = self.__cumulus.query_direct_to_private_api(self.__cumulus_lambda_prefix)
            if 'server_error' in cumulus_result:
                return {
                    'statusCode': 500,
                    'body': {'message': cumulus_result['server_error']}
                }
            if 'client_error' in cumulus_result:
                return {
                    'statusCode': 400,
                    'body': {'message': cumulus_result['client_error']}
                }
            cumulus_size = self.__get_size()
            return {
                'statusCode': 200,
                'body': {
                    'numberMatched': cumulus_size['total_size'],
                    'numberReturned': len(cumulus_result['results']),
                    'stac_version': '1.0.0',
                    'type': 'FeatureCollection',  # TODO correct name?
                    'links': self.__pagination_links,
                    'features': cumulus_result['results']
                }
            }
        except Exception as e:
            LOGGER.exception(f'unexpected error')
            return {
                'statusCode': 500,
                'body': {'message': f'unpredicted error: {str(e)}'}
            }
