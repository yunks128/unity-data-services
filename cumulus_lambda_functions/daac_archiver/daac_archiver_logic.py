import json
import os

from cumulus_lambda_functions.lib.uds_db.granules_db_index import GranulesDbIndex
from cumulus_lambda_functions.lib.aws.aws_sns import AwsSns
from cumulus_lambda_functions.lib.time_utils import TimeUtils
from cumulus_lambda_functions.lib.lambda_logger_generator import LambdaLoggerGenerator
from cumulus_lambda_functions.lib.uds_db.uds_collections import UdsCollections
from cumulus_lambda_functions.lib.uds_db.archive_index import UdsArchiveConfigIndex

LOGGER = LambdaLoggerGenerator.get_logger(__name__, LambdaLoggerGenerator.get_level_from_env())


class DaacArchiverLogic:
    def __init__(self):
        self.__es_url, self.__es_port = os.getenv('ES_URL'), int(os.getenv('ES_PORT', '443'))
        self.__archive_index_logic = UdsArchiveConfigIndex(self.__es_url, self.__es_port)
        self.__granules_index = GranulesDbIndex()
        self.__sns = AwsSns()

    def __extract_files(self, uds_cnm_json: dict, daac_config: dict):
        if 'archiving_types' not in daac_config or len(daac_config['archiving_types']) < 1:
            return uds_cnm_json['files']  # TODO remove missing md5?
        archiving_types = [{k['data_type']: [] if 'file_extension' not in k else k['file_extension'] for k in daac_config['archiving_types']}]
        result_files = []
        for each_file in uds_cnm_json['files']:
            """
            {
                "type": "data",
                "name": "abcd.1234.efgh.test_file05.nc",
                "uri": "https://uds-distribution-placeholder/uds-sbx-cumulus-staging/URN:NASA:UNITY:UDS_LOCAL_TEST:DEV:SNDR-SNPP_ATMS@L1B$OUTPUT___2403261440/URN:NASA:UNITY:UDS_LOCAL_TEST:DEV:SNDR-SNPP_ATMS@L1B$OUTPUT___2403261440:abcd.1234.efgh.test_file05/abcd.1234.efgh.test_file05.nc",
                "checksumType": "md5",
                "checksum": "unknown",
                "size": -1
            }
            """
            if each_file['type'] not in archiving_types:
                continue
            file_extensions = archiving_types[each_file['type']]
            if len(file_extensions) < 1:
                result_files.append(each_file)  # TODO remove missing md5?
            temp_filename = each_file['name'].upper().strip()
            if any([temp_filename.endswith(k.upper()) for k in file_extensions]):
                result_files.append(each_file)  # TODO remove missing md5?
        return result_files

    def send_to_daac(self, uds_cnm_json: dict):
        collection_identifier = UdsCollections.decode_identifier(uds_cnm_json['collection'])
        granule_identifier = UdsCollections.decode_identifier(uds_cnm_json['identifier'])  # This is normally meant to be for collection. Since our granule ID also has collection id prefix. we can use this.
        self.__archive_index_logic.set_tenant_venue(collection_identifier.tenant, collection_identifier.venue)
        daac_config = self.__archive_index_logic.percolate_document(uds_cnm_json['identifier'])
        if daac_config is None:
            LOGGER.debug(f'uds_cnm_json is not configured for archival. uds_cnm_json: {uds_cnm_json}')
            return
        # TODO This is currently not supporting more than 1 daac.
        try:
            self.__sns.set_topic_arn(daac_config['daac_sns_topic_arn'])
            daac_cnm_message = {
                "collection": daac_config['daac_collection_id'],
                "identifier": uds_cnm_json['identifier'],
                "submissionTime": f'{TimeUtils.get_current_time()}Z',
                "provider": collection_identifier.tenant,
                "version": "1.6.0",  # TODO this is hardcoded?
                "product": {
                    "name": granule_identifier.id,
                    "dataVersion": daac_config['daac_data_version'],
                    'files': self.__extract_files(uds_cnm_json, daac_config),
                }
            }
            self.__sns.publish_message(json.dumps(daac_cnm_message))
            self.__granules_index.update_entry(collection_identifier.tenant, collection_identifier.venue, {'archive_status': 'cnm_s_success'}, uds_cnm_json['identifier'])
        except Exception as e:
            LOGGER.exception(f'failed during archival process')
            self.__granules_index.update_entry(collection_identifier.tenant, collection_identifier.venue, {'archive_status': 'cnm_s_success'}, uds_cnm_json['identifier'])
        return

    def receive_from_daac(self, event_msg):
        return
