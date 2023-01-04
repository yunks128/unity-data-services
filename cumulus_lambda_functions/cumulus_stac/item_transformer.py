import json

from cumulus_lambda_functions.cumulus_stac.stac_transformer_abstract import StacTransformerAbstract
from cumulus_lambda_functions.lib.json_validator import JsonValidator
from cumulus_lambda_functions.lib.time_utils import TimeUtils

STAC_ITEM_SCHEMA = '''{
    "$schema": "http://json-schema.org/draft-07/schema#",
    "$id": "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/item.json#",
    "title": "STAC Item",
    "type": "object",
    "description": "This object represents the metadata for an item in a SpatioTemporal Asset Catalog.",
    "allOf": [
        {
            "$ref": "#/definitions/core"
        }
    ],
    "definitions": {
        "common_metadata": {
            "allOf": [
                {
                    "$ref": "basics.json"
                },
                {
                    "$ref": "datetime.json"
                },
                {
                    "$ref": "instrument.json"
                },
                {
                    "$ref": "licensing.json"
                },
                {
                    "$ref": "provider.json"
                }
            ]
        },
        "core": {
            "allOf": [
                {
                    "$ref": "https://geojson.org/schema/Feature.json"
                },
                {
                    "oneOf": [
                        {
                            "type": "object",
                            "required": [
                                "geometry",
                                "bbox"
                            ],
                            "properties": {
                                "geometry": {
                                    "$ref": "https://geojson.org/schema/Geometry.json"
                                },
                                "bbox": {
                                    "type": "array",
                                    "oneOf": [
                                        {
                                            "minItems": 4,
                                            "maxItems": 4
                                        },
                                        {
                                            "minItems": 6,
                                            "maxItems": 6
                                        }
                                    ],
                                    "items": {
                                        "type": "number"
                                    }
                                }
                            }
                        },
                        {
                            "type": "object",
                            "required": [
                                "geometry"
                            ],
                            "properties": {
                                "geometry": {
                                    "type": "null"
                                },
                                "bbox": {
                                    "not": {}
                                }
                            }
                        }
                    ]
                },
                {
                    "type": "object",
                    "required": [
                        "stac_version",
                        "id",
                        "links",
                        "assets",
                        "properties"
                    ],
                    "properties": {
                        "stac_version": {
                            "title": "STAC version",
                            "type": "string",
                            "const": "1.0.0"
                        },
                        "stac_extensions": {
                            "title": "STAC extensions",
                            "type": "array",
                            "uniqueItems": true,
                            "items": {
                                "title": "Reference to a JSON Schema",
                                "type": "string",
                                "format": "iri"
                            }
                        },
                        "id": {
                            "title": "Provider ID",
                            "description": "Provider item ID",
                            "type": "string",
                            "minLength": 1
                        },
                        "links": {
                            "title": "Item links",
                            "description": "Links to item relations",
                            "type": "array",
                            "items": {
                                "$ref": "#/definitions/link"
                            }
                        },
                        "assets": {
                            "$ref": "#/definitions/assets"
                        },
                        "properties": {
                            "allOf": [
                                {
                                    "$ref": "#/definitions/common_metadata"
                                },
                                {
                                    "anyOf": [
                                        {
                                            "required": [
                                                "datetime"
                                            ],
                                            "properties": {
                                                "datetime": {
                                                    "not": {
                                                        "type": "null"
                                                    }
                                                }
                                            }
                                        },
                                        {
                                            "required": [
                                                "datetime",
                                                "start_datetime",
                                                "end_datetime"
                                            ]
                                        }
                                    ]
                                }
                            ]
                        }
                    },
                    "if": {
                        "properties": {
                            "links": {
                                "contains": {
                                    "required": [
                                        "rel"
                                    ],
                                    "properties": {
                                        "rel": {
                                            "const": "collection"
                                        }
                                    }
                                }
                            }
                        }
                    },
                    "then": {
                        "required": [
                            "collection"
                        ],
                        "properties": {
                            "collection": {
                                "title": "Collection ID",
                                "description": "The ID of the STAC Collection this Item references to.",
                                "type": "string",
                                "minLength": 1
                            }
                        }
                    },
                    "else": {
                        "properties": {
                            "collection": {
                                "not": {}
                            }
                        }
                    }
                }
            ]
        },
        "link": {
            "type": "object",
            "required": [
                "rel",
                "href"
            ],
            "properties": {
                "href": {
                    "title": "Link reference",
                    "type": "string",
                    "format": "iri-reference",
                    "minLength": 1
                },
                "rel": {
                    "title": "Link relation type",
                    "type": "string",
                    "minLength": 1
                },
                "type": {
                    "title": "Link type",
                    "type": "string"
                },
                "title": {
                    "title": "Link title",
                    "type": "string"
                }
            }
        },
        "assets": {
            "title": "Asset links",
            "description": "Links to assets",
            "type": "object",
            "additionalProperties": {
                "$ref": "#/definitions/asset"
            }
        },
        "asset": {
            "allOf": [
                {
                    "type": "object",
                    "required": [
                        "href"
                    ],
                    "properties": {
                        "href": {
                            "title": "Asset reference",
                            "type": "string",
                            "format": "iri-reference",
                            "minLength": 1
                        },
                        "title": {
                            "title": "Asset title",
                            "type": "string"
                        },
                        "description": {
                            "title": "Asset description",
                            "type": "string"
                        },
                        "type": {
                            "title": "Asset type",
                            "type": "string"
                        },
                        "roles": {
                            "title": "Asset roles",
                            "type": "array",
                            "items": {
                                "type": "string"
                            }
                        }
                    }
                },
                {
                    "$ref": "#/definitions/common_metadata"
                }
            ]
        }
    }
}'''


CUMULUS_FILE_SCHEMA = {
    "type": "object",
    "required": ["bucket", "key", "fileName", "type", ],
    "properties": {
        "bucket": {"type": "string"},
        "key": {"type": "string"},
        "fileName": {"type": "string"},
        "type": {"type": "string"},
    }
}


class ItemTransformer(StacTransformerAbstract):
    def __init__(self):
        self.__stac_item_schema = json.loads(STAC_ITEM_SCHEMA)
        self.__cumulus_granule_schema = {}

    def __get_asset_name(self, input_dict):
        if input_dict['type'] == 'data':
            return 'data'
        if input_dict['type'] == 'metadata':
            filename = input_dict['fileName'].upper().strip()
            if filename.endswith('.CMR.XML'):
                return 'metadata__cmr'
            if filename.endswith('.PDS.XML'):
                return 'metadata__xml'
            return 'metadata__data'
        return input_dict['type']

    def __get_assets(self, input_dict):
        """
        Sample:
        {
            "bucket": "am-uds-dev-cumulus-internal",
            "key": "ATMS_SCIENCE_Group___1/P1570515ATMSSCIENCEAAT16032024518500.PDS",
            "size": 760,
            "fileName": "P1570515ATMSSCIENCEAAT16032024518500.PDS",
            "source": "data/SNPP_ATMS_Level0_T/ATMS_SCIENCE_Group/2016/031//P1570515ATMSSCIENCEAAT16032024518500.PDS",
            "type": "data"
        }
        :param input_dict:
        :return:
        """
        asset_dict = {
            'href': f"s3://{input_dict['bucket']}/{input_dict['key']}",
            'title': input_dict['fileName'],
            'description': input_dict['fileName'],
            # 'type': '',
            # 'roles': '',
        }
        return asset_dict

    def __get_datetime_from_source(self, source: dict, datetime_key: str):
        if datetime_key not in source:
            return '1970-01-01T00:00:00Z'
        return f"{source[datetime_key]}{'' if source[datetime_key].endswith('Z') else 'Z'}"

    def to_stac(self, source: dict) -> dict:
        """
        Sample: Cumulus granule
        {
            "published": false,
            "endingDateTime": "2016-01-31T19:59:59.991043",
            "status": "completed",
            "timestamp": 1648050501578,
            "createdAt": 1648050499079,
            "processingEndDateTime": "2022-03-23T15:48:20.869Z",
            "productVolume": 18096656,
            "timeToPreprocess": 20.302,
            "timeToArchive": 0,
            "productionDateTime": "2016-02-01T02:45:59.639000Z",
            "execution": "https://console.aws.amazon.com/states/home?region=us-west-2#/executions/details/arn:aws:states:us-west-2:884500545225:execution:am-uds-dev-cumulus-IngestGranule:ec602ca7-0243-44df-adc0-28fb8a486d54",
            "files": [
                {
                    "bucket": "am-uds-dev-cumulus-internal",
                    "key": "ATMS_SCIENCE_Group___1/P1570515ATMSSCIENCEAAT16032024518500.PDS",
                    "size": 760,
                    "fileName": "P1570515ATMSSCIENCEAAT16032024518500.PDS",
                    "source": "data/SNPP_ATMS_Level0_T/ATMS_SCIENCE_Group/2016/031//P1570515ATMSSCIENCEAAT16032024518500.PDS",
                    "type": "data"
                },
                {
                    "bucket": "am-uds-dev-cumulus-internal",
                    "key": "ATMS_SCIENCE_Group___1/P1570515ATMSSCIENCEAAT16032024518501.PDS",
                    "size": 18084600,
                    "fileName": "P1570515ATMSSCIENCEAAT16032024518501.PDS",
                    "source": "data/SNPP_ATMS_Level0_T/ATMS_SCIENCE_Group/2016/031//P1570515ATMSSCIENCEAAT16032024518501.PDS",
                    "type": "metadata"
                },
                {
                    "bucket": "am-uds-dev-cumulus-internal",
                    "key": "ATMS_SCIENCE_Group___1/P1570515ATMSSCIENCEAAT16032024518501.PDS.xml",
                    "size": 9547,
                    "fileName": "P1570515ATMSSCIENCEAAT16032024518501.PDS.xml",
                    "source": "data/SNPP_ATMS_Level0_T/ATMS_SCIENCE_Group/2016/031//P1570515ATMSSCIENCEAAT16032024518501.PDS.xml",
                    "type": "metadata"
                },
                {
                    "bucket": "am-uds-dev-cumulus-internal",
                    "key": "ATMS_SCIENCE_Group___1/P1570515ATMSSCIENCEAAT16032024518500.PDS.cmr.xml",
                    "size": 1749,
                    "fileName": "P1570515ATMSSCIENCEAAT16032024518500.PDS.cmr.xml",
                    "type": "metadata"
                }
            ],
            "processingStartDateTime": "2022-03-23T15:45:03.732Z",
            "updatedAt": 1648050501578,
            "beginningDateTime": "2016-01-31T18:00:00.009057",
            "provider": "snpp_provider_03",
            "granuleId": "P1570515ATMSSCIENCEAAT16032024518500.PDS",
            "collectionId": "ATMS_SCIENCE_Group___001",
            "duration": 197.993,
            "error": {},
            "lastUpdateDateTime": "2018-04-25T21:45:45.524053"
        }
        :param source:
        :return:
        """
        validation_result = JsonValidator(self.__cumulus_granule_schema).validate(source)
        if validation_result is not None:
            raise ValueError(f'invalid cumulus granule json: {validation_result}')

        cumulus_file_validator = JsonValidator(CUMULUS_FILE_SCHEMA)
        validated_files = [k for k in source['files'] if cumulus_file_validator.validate(k) is None]
        minimum_stac_item = {
            "stac_version": "1.0.0",
            "stac_extensions": [],
            "type": "Feature",
            "id": source['granuleId'],
            "bbox": [0, 0, 0, 0, ],
            "geometry": {
                "type": "Point",
                "coordinates": [0, 0]
            },
            "properties": {
                "datetime": f"{TimeUtils.decode_datetime(source['createdAt'], False)}Z",
                "start_datetime": self.__get_datetime_from_source(source, 'beginningDateTime'),
                "end_datetime": self.__get_datetime_from_source(source, 'endingDateTime'),
                "created": self.__get_datetime_from_source(source, 'productionDateTime'),
                "updated": f"{TimeUtils.decode_datetime(source['updatedAt'], False)}Z",
                # "created": source['processingEndDateTime'],  # TODO
            },
            "collection": source['collectionId'],
            "links": [
                {
                    "rel": "collection",
                    "href": ".",
                    # "type": "application/json",
                    # "title": "Simple Example Collection"
                }
            ],
            "assets": {self.__get_asset_name(k): self.__get_assets(k) for k in validated_files}
        }
        return minimum_stac_item

    def from_stac(self, source: dict) -> dict:
        return {}
