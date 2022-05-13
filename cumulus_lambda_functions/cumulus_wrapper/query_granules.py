import json

import requests
from cumulus_lambda_functions.cumulus_stac.item_transformer import ItemTransformer

from cumulus_lambda_functions.cumulus_wrapper.cumulus_base import CumulusBase


class GranulesQuery(CumulusBase):
    __granules_key = 'granules'
    __ending_time_key = 'endingDateTime'
    __beginning_time_key = 'beginningDateTime'

    def __init__(self, cumulus_base: str, cumulus_token: str):
        super().__init__(cumulus_base, cumulus_token)

    def with_bbox(self):
        return self

    def with_time_from(self, from_time):
        self._conditions.append(f'{self.__ending_time_key}__from={from_time}')
        return self

    def with_time_to(self, to_time):
        self._conditions.append(f'{self.__beginning_time_key}__from={to_time}')
        return self

    def with_time(self, input_time):
        self._conditions.append(f'{self.__beginning_time_key}__from={input_time}')
        self._conditions.append(f'{self.__ending_time_key}__to={input_time}')
        return self

    def with_time_range(self, from_time, to_time):
        """

        curl -k "$CUMULUS_BASEURL/granules?limit=1&beginningDateTime__from=2016-01-18T22:00:00&endingDateTime__to=2016-01-20T22:00:00" --header "Authorization: Bearer $cumulus_token"|jq
        :param beginning_dt:
        :param ending_dt:
        :return:
        """
        self._conditions.append(f'{self.__ending_time_key}__from={from_time}')
        self._conditions.append(f'{self.__beginning_time_key}__to={to_time}')
        return self

    def query(self):
        conditions_str = '&'.join(self._conditions)
        query_result = requests.get(url=f'{self.cumulus_base}/{self.__granules_key}?{conditions_str}', headers=self.get_base_headers())
        if query_result.status_code >= 500:
            return {'server_error': query_result.text}
        if query_result.status_code >= 400:
            return {'client_error': query_result.text}
        query_result = json.loads(query_result.content.decode())
        query_result = query_result['results']
        return {'results': [ItemTransformer().to_stac(k) for k in query_result]}
