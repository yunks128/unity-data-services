from cumulus_lambda_functions.cumulus_dapa_client.dapa_client import DapaClient
from cumulus_lambda_functions.stage_in_out.catalog_granules_abstract import CatalogGranulesAbstract
import json
import logging
import os


LOGGER = logging.getLogger(__name__)


class CatalogGranulesUnity(CatalogGranulesAbstract):
    PROVIDER_ID_KEY = 'PROVIDER_ID'
    UPLOADED_FILES_JSON = 'UPLOADED_FILES_JSON'
    VERIFY_SSL_KEY = 'VERIFY_SSL'

    def __init__(self) -> None:
        super().__init__()
        self.__provider_id = ''
        self.__uploaded_files_json = None
        self.__verify_ssl = True

    def __set_props_from_env(self):
        missing_keys = [k for k in [self.UPLOADED_FILES_JSON, self.PROVIDER_ID_KEY] if k not in os.environ]
        if len(missing_keys) > 0:
            raise ValueError(f'missing environment keys: {missing_keys}')
        self.__provider_id = os.environ.get(self.PROVIDER_ID_KEY)
        self.__uploaded_files_json = json.loads(os.environ.get(self.UPLOADED_FILES_JSON))
        self.__verify_ssl = os.environ.get(self.VERIFY_SSL_KEY, 'TRUE').strip().upper() == 'TRUE'
        return self

    def catalog(self, **kwargs):
        self.__set_props_from_env()
        dapa_body = {
            "provider_id": self.__provider_id,
            "features": self.__uploaded_files_json
        }
        dapa_client = DapaClient().with_verify_ssl(self.__verify_ssl)
        LOGGER.debug(f'dapa_body_granules: {dapa_body}')
        dapa_ingest_result = dapa_client.ingest_granules_w_cnm(dapa_body)
        return dapa_ingest_result
