import json
import logging
import os

from cumulus_lambda_functions.cumulus_dapa_client.dapa_client import DapaClient
from cumulus_lambda_functions.cumulus_stac.stac_utils import StacUtils
from cumulus_lambda_functions.stage_in_out.search_granules_abstract import SearchGranulesAbstract

LOGGER = logging.getLogger(__name__)


class SearchGranulesUnity(SearchGranulesAbstract):
    COLLECTION_ID_KEY = 'COLLECTION_ID'
    DOWNLOAD_DIR_KEY = 'DOWNLOAD_DIR'

    LIMITS_KEY = 'LIMITS'
    DATE_FROM_KEY = 'DATE_FROM'
    DATE_TO_KEY = 'DATE_TO'
    VERIFY_SSL_KEY = 'VERIFY_SSL'

    FILTER_ONLY_ASSETS = 'FILTER_ONLY_ASSETS'

    def __init__(self) -> None:
        super().__init__()
        self.__collection_id = ''
        self.__date_from = ''
        self.__date_to = ''
        self.__limit = 1000
        self.__verify_ssl = True
        self.__filter_results = True

    def __set_props_from_env(self):
        missing_keys = [k for k in [self.COLLECTION_ID_KEY] if k not in os.environ]
        if len(missing_keys) > 0:
            raise ValueError(f'missing environment keys: {missing_keys}')

        self.__collection_id = os.environ.get(self.COLLECTION_ID_KEY)
        if self.LIMITS_KEY not in os.environ:
            LOGGER.warning(f'missing {self.LIMITS_KEY}. using default: {self.__limit}')
        else:
            self.__limit = int(os.environ.get(self.LIMITS_KEY))

        self.__date_from = os.environ.get(self.DATE_FROM_KEY, '')
        self.__date_to = os.environ.get(self.DATE_TO_KEY, '')
        self.__verify_ssl = os.environ.get(self.VERIFY_SSL_KEY, 'TRUE').strip().upper() == 'TRUE'
        self.__filter_results = os.environ.get(self.FILTER_ONLY_ASSETS, 'TRUE').strip().upper() == 'TRUE'
        return self

    def search(self, **kwargs) -> str:
        self.__set_props_from_env()
        dapa_client = DapaClient().with_verify_ssl(self.__verify_ssl)
        granules_result = dapa_client.get_all_granules(self.__collection_id, self.__limit, self.__date_from, self.__date_to)
        granules_result = StacUtils.reduce_stac_list_to_data_links(granules_result) if self.__filter_results else granules_result
        return json.dumps(granules_result)
