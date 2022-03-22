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
        self.__input_file_list = []

    def __validate_input(self):
        result = JsonValidator(INPUT_EVENT_SCHEMA).validate(self.__event)
        if result is None:
            return
        raise ValueError(f'input json has validation errors: {result}')

    def __get_pds_metadata_file(self):
        self.__input_file_list = self.__event['cma']['event']['meta']['input_granules'][0]['files']
        for each_file in self.__input_file_list:
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
        LOGGER.error(f'input: {self.__event}')
        pds_metadata = PdsMetadata(xmltodict.parse(self.__read_pds_metadata_file())).load()
        echo_metadata = EchoMetadata(pds_metadata).load().echo_metadata
        echo_metadata_xml_str = xmltodict.unparse(echo_metadata, pretty=True)
        self.__s3.target_key = os.path.join(os.path.dirname(self.__s3.target_key), f'{pds_metadata.granule_id}.cmr.xml')
        self.__s3.upload_bytes(echo_metadata_xml_str.encode())
        # return {
        #     'files': ['example', 'mock', 'return'],
        #     'granules': self.__event
        # }
        return {
            "cumulus_meta": {
                "cumulus_version": "10.0.1",
                "execution_name": "c6d885dc-b4b2-4eb0-b22e-b6f58a7a0870",
                "message_source": "sfn",
                "queueExecutionLimits": {
                    "https://sqs.us-west-2.amazonaws.com/884500545225/am-uds-dev-cumulus-backgroundProcessing": 5
                },
                "state_machine": "arn:aws:states:us-west-2:884500545225:stateMachine:am-uds-dev-cumulus-IngestGranule",
                "system_bucket": "am-uds-dev-cumulus-internal",
                "workflow_start_time": 1646785175509,
                "parentExecutionArn": "arn:aws:states:us-west-2:884500545225:execution:am-uds-dev-cumulus-DiscoverGranules:885483b4-ba55-4db1-b197-661e1e595a45",
                "queueUrl": "arn:aws:sqs:us-west-2:884500545225:am-uds-dev-cumulus-startSF"
            },
            "exception": "None",
            "meta": {
                "buckets": {
                    "internal": {
                        "name": "am-uds-dev-cumulus-internal",
                        "type": "internal"
                    },
                    "protected": {
                        "name": "am-uds-dev-cumulus-protected",
                        "type": "protected"
                    }
                },
                "cmr": {
                    "clientId": "CHANGEME",
                    "cmrEnvironment": "UAT",
                    "cmrLimit": 100,
                    "cmrPageSize": 50,
                    "oauthProvider": "earthdata",
                    "passwordSecretName": "am-uds-dev-cumulus-message-template-cmr-password20220216072916956000000002",
                    "provider": "CHANGEME",
                    "username": "username"
                },
                "collection": {
                    "name": "ATMS_SCIENCE_Group_2011",
                    "version": "001",
                    "process": "modis",
                    # "beginningDateTime": "2000-01-01T00:00:00Z",
                    # "endingDateTime": "2000-01-01T02:00:00Z",
                    "granuleId": "^P[0-9]{3}[0-9]{4}[A-Z]{13}T[0-9]{12}0$",
                    "granuleIdExtraction": "(P[0-9]{3}[0-9]{4}[A-Z]{13}T[0-9]{12}0).+",
                    "sampleFileName": "P1570515ATMSSCIENCEAXT11344000000001.PDS",
                    "duplicateHandling": "replace",
                    "url_path": "{cmrMetadata.Granule.Collection.ShortName}___{cmrMetadata.Granule.Collection.VersionId}",
                    "provider_path": "/data/SNPP_ATMS_Level0_T/ATMS_SCIENCE_Group/2011/",
                    "files": [
                        {
                            "bucket": "internal",
                            "regex": "^P[0-9]{3}[0-9]{4}[A-Z]{13}T[0-9]{12}00.PDS$",
                            "sampleFileName": "P1570515ATMSSCIENCEAXT11344000000000.PDS",
                            "type": "data"
                        },
                        {
                            "bucket": "internal",
                            "regex": "^P[0-9]{3}[0-9]{4}[A-Z]{13}T[0-9]{12}01.PDS$",
                            "sampleFileName": "P1570515ATMSSCIENCEAXT11344000000001.PDS",
                            "type": "metadata"
                        },
                        {
                            "bucket": "internal",
                            "regex": "^P[0-9]{3}[0-9]{4}[A-Z]{13}T[0-9]{12}01.PDS.xml$",
                            "sampleFileName": "P1570515ATMSSCIENCEAXT11344000000001.PDS.xml",
                            "type": "metadata"
                        },
                        {
                            "bucket": "internal",
                            "regex": "^P[0-9]{3}[0-9]{4}[A-Z]{13}T[0-9]{12}00.PDS.cmr.xml$",
                            "sampleFileName": "P1570515ATMSSCIENCEAXT11344000000001.PDS.cmr.xml",
                            "type": "metadata"
                        },
                        {
                            "bucket": "internal",
                            "regex": "^P[0-9]{3}[0-9]{4}[A-Z]{13}T[0-9]{12}0.cmr.xml$",
                            "sampleFileName": "P1570515ATMSSCIENCEAXT11344000000001.PDS.xml",
                            "type": "metadata"
                        }
                    ],
                    "updatedAt": 1646326197526,
                    "createdAt": 1646258167624
                },
                "distribution_endpoint": "s3://am-uds-dev-cumulus-internal/",
                "launchpad": {
                    "api": "launchpadApi",
                    "certificate": "launchpad.pfx",
                    "passphraseSecretName": ""
                },
                "provider": {
                    "password": "AQICAHhSagsGDAl5tQWM010IEvxKgj2LcsNub5v5FHoRpOjXcQHFbE4iMnF/W0Y/NrsYvrfHAAAAajBoBgkqhkiG9w0BBwagWzBZAgEAMFQGCSqGSIb3DQEHATAeBglghkgBZQMEAS4wEQQMLaH13SdxPXREjXLtAgEQgCfA+lEu2c/xLTGwJsbtKlXJbKDy4pwV+rS3BnJqgBoLLMQZqOdoFhk=",
                    "host": "snppl0.gesdisc.eosdis.nasa.gov",
                    "updatedAt": 1646244053419,
                    "protocol": "https",
                    "createdAt": 1646244053419,
                    "encrypted": True,
                    "username": "AQICAHhSagsGDAl5tQWM010IEvxKgj2LcsNub5v5FHoRpOjXcQGRoY5EBMpvvyMASUowBM61AAAAYzBhBgkqhkiG9w0BBwagVDBSAgEAME0GCSqGSIb3DQEHATAeBglghkgBZQMEAS4wEQQM9OhRHwTuxiz74q4UAgEQgCDEHOhsVG6+LqXfnlw+Z3Wg9MDOCd9/K5/X5j3tPJYkaA==",
                    "allowedRedirects": [
                        "https://urs.earthdata.nasa.gov",
                        "urs.earthdata.nasa.gov"
                    ],
                    "id": "snpp_provider_02",
                    "globalConnectionLimit": 10
                },
                "stack": "am-uds-dev-cumulus",
                "template": "s3://am-uds-dev-cumulus-internal/am-uds-dev-cumulus/workflow_template.json",
                "workflow_name": "IngestGranule",
                "workflow_tasks": {
                    "SyncGranule": {
                        "name": "am-uds-dev-cumulus-FakeProcessing",
                        "version": "$LATEST",
                        "arn": "arn:aws:lambda:us-west-2:884500545225:function:am-uds-dev-cumulus-FakeProcessing"
                    }
                },
                "staticValue": "aStaticValue",
                "interpolatedValueStackName": "am-uds-dev-cumulus",
                "input_granules": [
                    {
                        "granuleId": "P1570515ATMSSCIENCEAXT1134912000000",
                        "dataType": "ATMS_SCIENCE_Group_2011",
                        "version": "001",
                        "files": [
                            {
                                "bucket": "am-uds-dev-cumulus-internal",
                                "key": "file-staging/am-uds-dev-cumulus/ATMS_SCIENCE_Group_2011___001/P1570515ATMSSCIENCEAXT11349120000000.PDS",
                                "source": "data/SNPP_ATMS_Level0_T/ATMS_SCIENCE_Group/2011/349//P1570515ATMSSCIENCEAXT11349120000000.PDS",
                                "fileName": "P1570515ATMSSCIENCEAXT11349120000000.PDS",
                                "type": "data",
                                "size": 744
                            },
                            {
                                "bucket": "am-uds-dev-cumulus-internal",
                                "key": "file-staging/am-uds-dev-cumulus/ATMS_SCIENCE_Group_2011___001/P1570515ATMSSCIENCEAXT11349120000001.PDS",
                                "source": "data/SNPP_ATMS_Level0_T/ATMS_SCIENCE_Group/2011/349//P1570515ATMSSCIENCEAXT11349120000001.PDS",
                                "fileName": "P1570515ATMSSCIENCEAXT11349120000001.PDS",
                                "type": "metadata",
                                "size": 18084600
                            },
                            {
                                "bucket": "am-uds-dev-cumulus-internal",
                                "key": "file-staging/am-uds-dev-cumulus/ATMS_SCIENCE_Group_2011___001/P1570515ATMSSCIENCEAXT11349120000001.PDS.xml",
                                "source": "data/SNPP_ATMS_Level0_T/ATMS_SCIENCE_Group/2011/349//P1570515ATMSSCIENCEAXT11349120000001.PDS.xml",
                                "fileName": "P1570515ATMSSCIENCEAXT11349120000001.PDS.xml",
                                "type": "metadata",
                                "size": 9526
                            }
                        ],
                        "sync_granule_duration": 20302,
                        "createdAt": 1646935567596
                    }
                ],
                "process": "modis"
            },
            "payload": {
                "granules": [
                    {
                        "granuleId": pds_metadata.granule_id,
                        "dataType": pds_metadata.collection_name,
                        "version": f'{pds_metadata.collection_version}',
                        # "beginningDateTime": pds_metadata.beginning_dt,
                        # "endingDateTime": pds_metadata.ending_dt,
                        "beginningDateTime": '2022-12-15T12:00:00.009054',
                        "endingDateTime": '2022-12-15T12:00:00.009054',
                        "files": self.__input_file_list + [{
                                "key": self.__s3.target_key,
                                "fileName": os.path.basename(self.__s3.target_key),
                                "bucket": self.__s3.target_bucket,
                                "size": int(self.__s3.get_size()),
                            }],
                        "sync_granule_duration": 20302,
                        "createdAt": 1646935567596
                    }
                ]
            },
            # "payload": [
            #     "s3://am-uds-dev-cumulus-internal/file-staging/am-uds-dev-cumulus/ATMS_SCIENCE_Group_2011___001/P1570515ATMSSCIENCEAXT11349120000000.PDS",
            #     "s3://am-uds-dev-cumulus-internal/file-staging/am-uds-dev-cumulus/ATMS_SCIENCE_Group_2011___001/P1570515ATMSSCIENCEAXT11349120000001.PDS",
            #     "s3://am-uds-dev-cumulus-internal/file-staging/am-uds-dev-cumulus/ATMS_SCIENCE_Group_2011___001/P1570515ATMSSCIENCEAXT11349120000001.PDS.xml",
            #     f's3://{self.__s3.target_bucket}/{self.__s3.target_key}',
            # ],
            "task_config": {
                "inputGranules": "{$.meta.input_granules}",
                "granuleIdExtraction": "{$.meta.collection.granuleIdExtraction}"
            },
            # "task_config": {
            #     "bucket": "{$.meta.buckets.internal.name}",
            #     "collection": "{$.meta.collection}",
            #     "cumulus_message": {
            #         "outputs": [
            #             {
            #                 "source": "{$.files}",
            #                 "destination": "{$.payload}"
            #             }
            #         ]
            #     }
            # }
        }
