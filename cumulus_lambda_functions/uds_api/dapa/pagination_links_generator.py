import os
from copy import deepcopy

from cumulus_lambda_functions.uds_api.web_service_constants import WebServiceConstants

from cumulus_lambda_functions.lib.lambda_logger_generator import LambdaLoggerGenerator
from fastapi import Request
LOGGER = LambdaLoggerGenerator.get_logger(__name__, LambdaLoggerGenerator.get_level_from_env())


class PaginationLinksGenerator:
    def __init__(self, request: Request, custom_params: dict = {}):
        self.__default_limit = 10
        self.__request = request
        self.__org_query_params = {k: v for k, v in request.query_params.items()}
        self.__org_query_params = {**self.__org_query_params, **custom_params}
        self.__base_url = os.environ.get(WebServiceConstants.BASE_URL, f'{self.__request.url.scheme}://{self.__request.url.netloc}')
        self.__base_url = self.__base_url[:-1] if self.__base_url.endswith('/') else self.__base_url
        self.__base_url = self.__base_url if self.__base_url.startswith('http') else f'https://{self.__base_url}'


    def __get_current_page(self):
        try:
            # requesting_base_url = f"{self.__request.base_url}{self.__request.url.path}"
            requesting_base_url = f"{self.__base_url}{self.__request.url.path}"
            new_queries = deepcopy(self.__org_query_params)
            limit = int(new_queries['limit'] if 'limit' in new_queries else self.__default_limit)
            offset = int(new_queries['offset'] if 'offset' in new_queries else 0)
            new_queries['limit'] = limit
            new_queries['offset'] = offset
            requesting_url = f"{requesting_base_url}?{'&'.join([f'{k}={v}' for k, v in new_queries.items()])}"
        except Exception as e:
            LOGGER.exception(f'error while getting current page URL')
            return f'unable to get current page URL, {str(e)}'
        return requesting_url

    def __get_next_page(self):
        try:
            requesting_base_url = f"{self.__base_url}{self.__request.url.path}"
            new_queries = deepcopy(self.__org_query_params)
            limit = int(new_queries['limit'] if 'limit' in new_queries else self.__default_limit)
            if limit == 0:
                return ''
            offset = int(new_queries['offset'] if 'offset' in new_queries else 0)
            offset += limit
            new_queries['limit'] = limit
            new_queries['offset'] = offset
            requesting_url = f"{requesting_base_url}?{'&'.join([f'{k}={v}' for k, v in new_queries.items()])}"
        except Exception as e:
            LOGGER.exception(f'error while getting next page URL')
            return f'unable to get next page URL, {str(e)}'
        return requesting_url

    def __get_prev_page(self):
        try:
            requesting_base_url = f"{self.__base_url}{self.__request.url.path}"
            new_queries = deepcopy(self.__org_query_params)
            limit = int(new_queries['limit'] if 'limit' in new_queries else self.__default_limit)
            if limit == 0:
                return ''
            offset = int(new_queries['offset'] if 'offset' in new_queries else 0)
            offset -= limit
            if offset < 0:
                offset = 0
            new_queries['limit'] = limit
            new_queries['offset'] = offset
            requesting_url = f"{requesting_base_url}?{'&'.join([f'{k}={v}' for k, v in new_queries.items()])}"
        except Exception as e:
            LOGGER.exception(f'error while getting previous page URL')
            return f'unable to get previous page URL, {str(e)}'
        return requesting_url

    def generate_pagination_links(self):
        try:
            pagination_links = [
            {'rel': 'self', 'href': self.__get_current_page()},
            {'rel': 'root', 'href': str(self.__request.base_url)},
            {'rel': 'next', 'href': self.__get_next_page()},
            {'rel': 'prev', 'href': self.__get_prev_page()},
        ]
        except Exception as e:
            LOGGER.exception(f'error while generating pagination links')
            return [{'message': f'error while generating pagination links: {str(e)}'}]
        return pagination_links
