import os

import xmltodict

from cumulus_lambda_functions.lib.aws.aws_s3 import AwsS3
from cumulus_lambda_functions.lib.json_validator import JsonValidator
from cumulus_lambda_functions.lib.lambda_logger_generator import LambdaLoggerGenerator
from cumulus_lambda_functions.snpp_lvl0_generate_cmr.echo_metadata import EchoMetadata
from cumulus_lambda_functions.snpp_lvl0_generate_cmr.pds_metadata import PdsMetadata

LOGGER = LambdaLoggerGenerator.get_logger(__name__, LambdaLoggerGenerator.get_level_from_env())

INPUT_EVENT_SCHEMA = {
    "type": "object",
    "properties": {
        "cma": {
            "type": "object",
            "properties": {
                "event": {
                    "type": "object",
                    "properties": {
                        "meta": {
                            "type": "object",
                            "properties": {
                                "input_granules": {
                                    "type": "array",
                                    "minItems": 1,
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "granuleId": {
                                                "type": "string"
                                            },
                                            "files": {
                                                "type": "array",
                                                "minItems": 1,
                                                "items": {
                                                    "type": "object",
                                                    "properties": {
                                                        "bucket": {
                                                            "type": "string"
                                                        },
                                                        "key": {
                                                            "type": "string"
                                                        },
                                                        "source": {
                                                            "type": "string"
                                                        },
                                                        "fileName": {
                                                            "type": "string"
                                                        },
                                                        "type": {
                                                            "type": "string"
                                                        },
                                                        "size": {
                                                            "type": "number"
                                                        }
                                                    },
                                                    "required": [
                                                        "bucket",
                                                        "key",
                                                        "type"
                                                    ]
                                                }
                                            }
                                        },
                                        "required": [
                                            "granuleId",
                                            "files"
                                        ]
                                    }
                                }
                            },
                            "required": [
                                "input_granules"
                            ]
                        }
                    },
                    "required": [
                        "meta"
                    ]
                }
            },
            "required": []
        }
    },
    "required": [
        "cma"
    ]
}


class GenerateCmr:
    def __init__(self, event):
        self.__event = event
        self.__s3 = AwsS3()
        self._pds_file_dict = None

    def __validate_input(self):
        result = JsonValidator(INPUT_EVENT_SCHEMA).validate(self.__event)
        if result is None:
            return
        raise ValueError(f'input json has validation errors: {result}')

    def __get_pds_metadata_file(self):
        for each_file in self.__event['cma']['event']['meta']['input_granules'][0]['files']:
            LOGGER.debug(f'checking file: {each_file}')
            if each_file['key'].upper().endswith('1.PDS.XML'):
                return each_file
        return None

    def __read_pds_metadata_file(self):
        self._pds_file_dict = self.__get_pds_metadata_file()
        if self._pds_file_dict is None:
            raise ValueError('missing PDS metadata file')
        self.__s3.target_bucket = self._pds_file_dict['bucket']
        self.__s3.target_key = self._pds_file_dict['key']
        return self.__s3.read_small_txt_file()

    def start(self):
        self.__validate_input()
        pds_metadata = PdsMetadata(xmltodict.parse(self.__read_pds_metadata_file())).load()
        echo_metadata = EchoMetadata(pds_metadata).load().echo_metadata
        echo_metadata_xml_str = xmltodict.unparse(echo_metadata, pretty=True)
        self.__s3.target_key = os.path.join(os.path.dirname(self.__s3.target_key), f'{pds_metadata.granule_id}.cmr.xml')
        self.__s3.upload_bytes(echo_metadata_xml_str.encode())
        return {
            'files': [],
            'granules': self.__event
        }
