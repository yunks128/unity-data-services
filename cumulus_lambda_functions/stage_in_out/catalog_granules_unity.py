from cumulus_lambda_functions.cumulus_dapa_client.dapa_client import DapaClient
from cumulus_lambda_functions.lib.time_utils import TimeUtils
from cumulus_lambda_functions.stage_in_out.catalog_granules_abstract import CatalogGranulesAbstract
import logging
import os

from cumulus_lambda_functions.stage_in_out.cataloging_granules_status_checker import CatalogingGranulesStatusChecker

LOGGER = logging.getLogger(__name__)


class CatalogGranulesUnity(CatalogGranulesAbstract):
    PROVIDER_ID_KEY = 'PROVIDER_ID'
    VERIFY_SSL_KEY = 'VERIFY_SSL'

    def __init__(self) -> None:
        super().__init__()
        self.__provider_id = ''
        self.__verify_ssl = True

    def __set_props_from_env(self):
        missing_keys = [k for k in [self.UPLOADED_FILES_JSON, self.PROVIDER_ID_KEY] if k not in os.environ]
        if len(missing_keys) > 0:
            raise ValueError(f'missing environment keys: {missing_keys}')
        self._retrieve_stac_json()
        self.__provider_id = os.environ.get(self.PROVIDER_ID_KEY)
        self.__verify_ssl = os.environ.get(self.VERIFY_SSL_KEY, 'TRUE').strip().upper() == 'TRUE'
        return self

    def catalog(self, **kwargs):
        self.__set_props_from_env()
        dapa_body = {
            "provider_id": self.__provider_id,
            "features": self._uploaded_files_json
        }
        dapa_client = DapaClient().with_verify_ssl(self.__verify_ssl)
        LOGGER.debug(f'dapa_body_granules: {dapa_body}')
        dapa_ingest_result = dapa_client.ingest_granules_w_cnm(dapa_body)
        extracted_ids = [k['id'] for k in self._uploaded_files_json]
        LOGGER.debug(f'checking following IDs: {extracted_ids}')
        status_checker = CatalogingGranulesStatusChecker(self._uploaded_files_json[0]['collection'],
                                                         extracted_ids,
                                                         TimeUtils().get_datetime_obj().timestamp(),
                                                         30,
                                                         10)
        status_result = status_checker.verify_n_times()
        registered_granules = dapa_client.get_granules(collection_id=self._uploaded_files_json[0]['collection'], filters={
            'in': {
                'value': {'property': 'id'},
                'list': extracted_ids
            }
        })

        print(registered_granules)
        return dapa_ingest_result
