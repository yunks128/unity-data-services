import json
import os

from cumulus_lambda_functions.lib.uds_db.uds_collections import UdsCollections

from cumulus_lambda_functions.metadata_stac_generate_cmr.stac_input_metadata import StacInputMetadata

from cumulus_lambda_functions.lib.aws.aws_s3 import AwsS3

from cumulus_lambda_functions.lib.lambda_logger_generator import LambdaLoggerGenerator

from cumulus_lambda_functions.lib.json_validator import JsonValidator

from cumulus_lambda_functions.lib.aws.aws_message_transformers import AwsMessageTransformers
from cumulus_lambda_functions.uds_api.dapa.granules_db_index import GranulesDbIndex

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
        self.__cumulus_record = {}
        self.__file_postfixes = os.getenv('FILE_POSTFIX', 'STAC.JSON')
        self.__file_postfixes = [k.upper().strip() for k in self.__file_postfixes.split(',')]
        self.__input_file_list = []
        self.__s3 = AwsS3()


    def __get_pds_metadata_file(self):
        self.__input_file_list = self.__cumulus_record['files']
        stac_metadata_file = None
        for each_file in self.__input_file_list:
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
                stac_metadata_file = each_file
        return stac_metadata_file

    def __read_pds_metadata_file(self):
        pds_file_dict = self.__get_pds_metadata_file()
        if pds_file_dict is None:
            raise ValueError('missing PDS metadata file')
        self.__s3.target_bucket = pds_file_dict['bucket']
        self.__s3.target_key = pds_file_dict['key']
        return self.__s3.read_small_txt_file()

    def start(self):
        incoming_msg = AwsMessageTransformers().sqs_sns(self.__event)
        result = JsonValidator(self.CUMULUS_SCHEMA).validate(incoming_msg)
        if result is not None:
            raise ValueError(f'input json has CUMULUS validation errors: {result}')
        self.__cumulus_record = incoming_msg['record']
        if len(self.__cumulus_record['files']) < 1:
            # TODO ingest updating stage?
            return
        stac_input_meta = StacInputMetadata(json.loads(self.__read_pds_metadata_file()))
        granules_metadata_props = stac_input_meta.start()
        custom_properties = stac_input_meta.custom_properties
        self.__cumulus_record = {
            **self.__cumulus_record,
            **custom_properties,
        }
        collection_identifier = UdsCollections.decode_identifier(self.__cumulus_record['collectionId'])
        GranulesDbIndex().add_entry(collection_identifier.tenant,
                                    collection_identifier.venue,
                                    self.__cumulus_record,
                                    self.__cumulus_record['granuleId']
                                    )
        return self
