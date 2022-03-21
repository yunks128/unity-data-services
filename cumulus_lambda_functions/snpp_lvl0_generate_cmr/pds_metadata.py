import xmltodict

from cumulus_lambda_functions.lib.json_validator import JsonValidator
from cumulus_lambda_functions.lib.lambda_logger_generator import LambdaLoggerGenerator

LOGGER = LambdaLoggerGenerator.get_logger(__name__, LambdaLoggerGenerator.get_level_from_env())
PDS_METADATA_SCHEMA = {
    "type": "object",
    "properties": {
        "S4PAGranuleMetaDataFile": {
            "type": "object",
            "properties": {
                "DataGranule": {
                    "type": "object",
                    "properties": {
                        "GranuleID": {"type": "string"},
                        "ProductionDateTime": {"type": "string"},
                        "InsertDateTime": {"type": "string"},
                    },
                    "required": ["GranuleID", "InsertDateTime", "ProductionDateTime"]
                },
                "RangeDateTime": {
                    "type": "object",
                    "properties": {
                        "RangeEndingDate": {"type": "string"},
                        "RangeEndingTime": {"type": "string"},
                        "RangeBeginningDate": {"type": "string"},
                        "RangeBeginningTime": {"type": "string"},
                    },
                    "required": ["RangeEndingDate", "RangeEndingTime", "RangeBeginningDate", "RangeBeginningTime"]

                },
                "CollectionMetaData": {
                    "type": "object",
                    "properties": {
                        "ShortName": {"type": "string"},
                        "VersionID": {"type": "string"}
                    },
                    "required": ["ShortName", "VersionID"]

                },
            },
            "required": ["RangeDateTime"]
        }
    },
    "required": ["S4PAGranuleMetaDataFile"]
}


class PdsMetadata:
    def __init__(self, input_pds_metadata: dict):
        """
        :param input_pds_metadata_str: str - XML string
        """
        self.__shit = None
        self.__input_pds_metadata = input_pds_metadata

        self.__beginning_dt = None
        self.__ending_dt = None
        self.__collection_name = None
        self.__collection_version = None
        self.__granule_id = None
        self.__prod_dt = None
        self.__insert_dt = None

    @property
    def beginning_dt(self):
        return self.__beginning_dt

    @beginning_dt.setter
    def beginning_dt(self, val):
        """
        :param val:
        :return: None
        """
        self.__beginning_dt = val
        return

    @property
    def ending_dt(self):
        return self.__ending_dt

    @ending_dt.setter
    def ending_dt(self, val):
        """
        :param val:
        :return: None
        """
        self.__ending_dt = val
        return

    @property
    def collection_name(self):
        return self.__collection_name

    @collection_name.setter
    def collection_name(self, val):
        """
        :param val:
        :return: None
        """
        self.__collection_name = val
        return

    @property
    def collection_version(self):
        return self.__collection_version

    @collection_version.setter
    def collection_version(self, val):
        """
        :param val:
        :return: None
        """
        self.__collection_version = val
        return

    @property
    def granule_id(self):
        return self.__granule_id

    @granule_id.setter
    def granule_id(self, val):
        """
        :param val:
        :return: None
        """
        self.__granule_id = val
        return

    @property
    def prod_dt(self):
        return self.__prod_dt

    @prod_dt.setter
    def prod_dt(self, val):
        """
        :param val:
        :return: None
        """
        self.__prod_dt = val
        return

    @property
    def insert_dt(self):
        return self.__insert_dt

    @insert_dt.setter
    def insert_dt(self, val):
        """
        :param val:
        :return: None
        """
        self.__insert_dt = val
        return

    def __validate_pds_metadata(self):
        result = JsonValidator(PDS_METADATA_SCHEMA).validate(self.__input_pds_metadata)
        if result is None:
            return
        raise ValueError(f'pds metadata has validation errors: {result}')

    def __load_time_range(self):
        range_dt = self.__input_pds_metadata['S4PAGranuleMetaDataFile']['RangeDateTime']
        self.__beginning_dt = f"{range_dt['RangeBeginningDate']}T{range_dt['RangeBeginningTime']}"
        self.__ending_dt = f"{range_dt['RangeEndingDate']}T{range_dt['RangeEndingTime']}"
        return

    def __load_collection_metadata(self):
        collection_met = self.__input_pds_metadata['S4PAGranuleMetaDataFile']['CollectionMetaData']
        self.__collection_name = collection_met['ShortName']
        self.__collection_version = collection_met['VersionID']
        return

    def __load_granule_metadata(self):
        granule_met = self.__input_pds_metadata['S4PAGranuleMetaDataFile']['DataGranule']
        self.__granule_id = granule_met['GranuleID']
        self.__prod_dt = granule_met['ProductionDateTime']
        self._insert_dt = granule_met['InsertDateTime']
        return

    def load(self):
        self.__validate_pds_metadata()
        self.__load_time_range()
        self.__load_granule_metadata()
        return self
