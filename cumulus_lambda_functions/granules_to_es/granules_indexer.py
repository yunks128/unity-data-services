import json
import os
from time import sleep

from cumulus_lambda_functions.daac_archiver.daac_archiver_logic import DaacArchiverLogic
from cumulus_lambda_functions.lib.utils.file_utils import FileUtils

from cumulus_lambda_functions.cumulus_stac.item_transformer import ItemTransformer

from cumulus_lambda_functions.lib.uds_db.uds_collections import UdsCollections

from cumulus_lambda_functions.metadata_stac_generate_cmr.stac_input_metadata import StacInputMetadata

from cumulus_lambda_functions.lib.aws.aws_s3 import AwsS3

from cumulus_lambda_functions.lib.lambda_logger_generator import LambdaLoggerGenerator

from cumulus_lambda_functions.lib.json_validator import JsonValidator

from cumulus_lambda_functions.lib.aws.aws_message_transformers import AwsMessageTransformers
from cumulus_lambda_functions.lib.uds_db.granules_db_index import GranulesDbIndex

LOGGER = LambdaLoggerGenerator.get_logger(__name__, LambdaLoggerGenerator.get_level_from_env())


class GranulesIndexer:
    CUMULUS_SCHEMA = {
        'type': 'object',
        'required': ['event', 'record'],
        'properties': {
            'event': {'type': 'string'},
            'record': {'type': 'object'},
        }
    }

    def __init__(self, event) -> None:
        self.__event = event
        LOGGER.debug(f'event: {event}')
        self.__cumulus_record = {}
        self.__file_postfixes = os.getenv('FILE_POSTFIX', 'STAC.JSON')
        self.__valid_filetype_name = os.getenv('VALID_FILETYPE', 'metadata').lower()
        self.__file_postfixes = [k.upper().strip() for k in self.__file_postfixes.split(',')]
        self.__input_file_list = []
        self.__s3 = AwsS3()

    def __get_potential_files(self):
        potential_files = []
        self.__input_file_list = self.__cumulus_record['files']
        for each_file in self.__input_file_list:
            if 'type' in each_file and each_file['type'].strip().lower() != self.__valid_filetype_name:
                LOGGER.debug(f'Not metadata. skipping {each_file}')
                continue
            if 'fileName' not in each_file and 'name' in each_file:  # add fileName if there is only name
                each_file['fileName'] = each_file['name']
            if 'url_path' in each_file:
                s3_bucket, s3_key = self.__s3.split_s3_url(each_file['url_path'])
                each_file['bucket'] = s3_bucket
                each_file['key'] = s3_key
            LOGGER.debug(f'checking file: {each_file}')
            file_key_upper = each_file['key'].upper().strip()
            LOGGER.debug(f'checking file_key_upper: {file_key_upper} against {self.__file_postfixes}')
            if any([file_key_upper.endswith(k) for k in self.__file_postfixes]):
                potential_files.append(each_file)
        return potential_files

    def __read_pds_metadata_file(self, potential_file):
        self.__s3.target_bucket = potential_file['bucket']
        self.__s3.target_key = potential_file['key']
        return self.__s3.read_small_txt_file()

    def __get_cnm_response_json_file(self, potential_file, granule_id):
        LOGGER.debug(f'attempting to retrieve cnm response from : {granule_id} & {potential_file}')
        current_bucket, current_key = potential_file['bucket'], potential_file['key']
        cnm_response_keys = [k for k, _ in self.__s3.get_child_s3_files(current_bucket, os.path.dirname(current_key)) if k.lower().endswith('.cnm.json')]
        if len(cnm_response_keys) < 1:
            LOGGER.debug(f'missing cnm response file: {os.path.dirname(current_key)}.. trying again in 30 second.')
            sleep(30)  # waiting 30 second. should be enough.
            cnm_response_keys = [k for k, _ in self.__s3.get_child_s3_files(current_bucket, os.path.dirname(current_key)) if k.lower().endswith('.cnm.json')]
            if len(cnm_response_keys) < 1:
                LOGGER.debug(f'missing cnm response file after 2nd try: {os.path.dirname(current_key)}.. quitting.')
                return None
        if len(cnm_response_keys) > 1:
            LOGGER.warning(f'more than 1 cnm response file: {cnm_response_keys}')
        cnm_response_keys = cnm_response_keys[0]
        LOGGER.debug(f'cnm_response_keys: {cnm_response_keys}')
        local_file = self.__s3.set_s3_url(f's3://{current_bucket}/{cnm_response_keys}').download('/tmp')
        return FileUtils.read_json(local_file)

    def start(self):
        incoming_msg = AwsMessageTransformers().sqs_sns(self.__event)
        result = JsonValidator(self.CUMULUS_SCHEMA).validate(incoming_msg)
        if result is not None:
            raise ValueError(f'input json has CUMULUS validation errors: {result}')
        self.__cumulus_record = incoming_msg['record']
        if len(self.__cumulus_record['files']) < 1:
            # TODO ingest updating stage?
            return
        stac_input_meta = None
        potential_files = self.__get_potential_files()
        LOGGER.debug(f'potential_files: {potential_files}')
        for each_potential_file in potential_files:
            try:
                LOGGER.debug(f'trying each_potential_file: {each_potential_file}')
                stac_input_meta = StacInputMetadata(json.loads(self.__read_pds_metadata_file(each_potential_file)))
                granules_metadata_props = stac_input_meta.start()
                break
            except:
                LOGGER.exception(f'most likely not a STAC file: {each_potential_file}')
        if stac_input_meta is not None:
            self.__cumulus_record['custom_metadata'] = stac_input_meta.custom_properties
        else:
            LOGGER.warning(f'unable to find STAC JSON file in {potential_files}')
        stac_item = ItemTransformer().to_stac(self.__cumulus_record)
        if 'bbox' in stac_item:
            stac_item['bbox'] = GranulesDbIndex.to_es_bbox(stac_item['bbox'])
        collection_identifier = UdsCollections.decode_identifier(self.__cumulus_record['collectionId'])
        LOGGER.debug(f'stac_item: {stac_item}')
        GranulesDbIndex().add_entry(collection_identifier.tenant,
                                    collection_identifier.venue,
                                    stac_item,
                                    self.__cumulus_record['granuleId']
                                    )
        LOGGER.debug(f'added to GranulesDbIndex')
        cnm_response = self.__get_cnm_response_json_file(potential_files[0], self.__cumulus_record['granuleId'])
        if cnm_response is None:
            LOGGER.error(f'no CNM Response file. Not continuing to DAAC Archiving')
            return self
        DaacArchiverLogic().send_to_daac_internal(cnm_response)
        return self
